package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoClient   *mongo.Client
	database      string
	requestTimeout time.Duration
	defaultLimit   int64 = 100
	maxLimit       int64 = 1000
)

// getEnv retrieves environment variable with fallback default
func getEnv(key, defaultVal string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultVal
}

// parseInt64 safely converts string to int64 with default fallback
func parseInt64(s string, defaultVal int64) int64 {
	if s == "" {
		return defaultVal
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultVal
	}
	return n
}

// parseInt safely converts string to int with default fallback
func parseInt(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return n
}

// parseBool converts string to boolean
func parseBool(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes" || s == "on"
}

// connectMongoDB establishes connection to MongoDB with timeout and ping
func connectMongoDB(ctx context.Context, uri string) (*mongo.Client, error) {
	maxPoolSize := uint64(parseInt(getEnv("MONGO_MAX_POOL_SIZE", "50"), 50))
	serverSelectTimeout := time.Duration(parseInt(getEnv("MONGO_SERVER_SELECT_TIMEOUT_MS", "5000"), 5000)) * time.Millisecond

	clientOpts := options.Client().
		ApplyURI(uri).
		SetMaxPoolSize(maxPoolSize).
		SetServerSelectionTimeout(serverSelectTimeout)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}

	// Verify connection with ping
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}

	return client, nil
}

// getCollection returns MongoDB collection from database
func getCollection(collectionName string) *mongo.Collection {
	return mongoClient.Database(database).Collection(collectionName)
}

// parseOperator extracts field name and MongoDB operator from key with suffix notation
// Supports: field__gt, field__gte, field__lt, field__lte, field__ne, field__regex, field__exists
func parseOperator(key string) (field, operator string, hasOperator bool) {
	suffixes := []string{"__gt", "__gte", "__lt", "__lte", "__ne", "__regex", "__exists", "__in"}
	for _, suffix := range suffixes {
		if strings.HasSuffix(key, suffix) {
			return strings.TrimSuffix(key, suffix), strings.TrimPrefix(suffix, "__"), true
		}
	}
	return key, "", false
}

// inferType converts string value to appropriate BSON type
// Supports: ObjectID (_id field), int64, float64, bool, RFC3339 time, string
func inferType(key, value string) interface{} {
	// Special handling for _id field as ObjectID
	if key == "_id" && len(value) == 24 {
		if oid, err := primitive.ObjectIDFromHex(value); err == nil {
			return oid
		}
	}

	// Try integer
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}

	// Try float
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}

	// Try boolean
	if b, err := strconv.ParseBool(strings.ToLower(value)); err == nil {
		return b
	}

	// Try RFC3339 time
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t
	}

	// Default to string
	return value
}

// buildFilter constructs BSON filter from URL query parameters
// Skips control parameters: limit, skip, sort, fields, upsert
// Supports comma-separated values as $in operator
func buildFilter(queries map[string]string) bson.M {
	filter := bson.M{}
	controlParams := map[string]bool{
		"limit": true, "skip": true, "sort": true, "fields": true, "upsert": true,
	}

	for key, rawValue := range queries {
		// Skip control parameters
		if controlParams[key] {
			continue
		}

		field, operator, hasOperator := parseOperator(key)

		// Handle comma-separated values as $in operator
		if !hasOperator && strings.Contains(rawValue, ",") {
			values := strings.Split(rawValue, ",")
			inArray := make([]interface{}, 0, len(values))
			for _, v := range values {
				inArray = append(inArray, inferType(field, strings.TrimSpace(v)))
			}
			filter[field] = bson.M{"$in": inArray}
			continue
		}

		value := inferType(field, rawValue)

		if hasOperator {
			operatorMap, ok := filter[field].(bson.M)
			if !ok {
				operatorMap = bson.M{}
			}

			switch operator {
			case "regex":
				operatorMap["$regex"] = value
			case "exists":
				operatorMap["$exists"] = parseBool(rawValue)
			default:
				operatorMap["$"+operator] = value
			}
			filter[field] = operatorMap
			continue
		}

		filter[field] = value
	}

	return filter
}

// parseSort converts comma-separated sort string to BSON sort document
// Format: field1,-field2,+field3 (- for descending, + or nothing for ascending)
func parseSort(sortStr string) bson.D {
	if sortStr == "" {
		return nil
	}

	fields := strings.Split(sortStr, ",")
	sortDoc := bson.D{}

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		direction := int32(1)
		if strings.HasPrefix(field, "-") {
			direction = -1
			field = strings.TrimPrefix(field, "-")
		} else if strings.HasPrefix(field, "+") {
			field = strings.TrimPrefix(field, "+")
		}

		sortDoc = append(sortDoc, bson.E{Key: field, Value: direction})
	}

	return sortDoc
}

// parseProjection converts comma-separated fields string to BSON projection
func parseProjection(fieldsStr string) bson.M {
	if fieldsStr == "" {
		return nil
	}

	fields := strings.Split(fieldsStr, ",")
	projection := bson.M{}

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			projection[field] = 1
		}
	}

	return projection
}

// errorResponse sends JSON error response
func errorResponse(c *fiber.Ctx, statusCode int, message string, err error) error {
	response := fiber.Map{
		"error":   message,
		"success": false,
	}
	if err != nil {
		response["details"] = err.Error()
	}
	return c.Status(statusCode).JSON(response)
}

// setupRoutes configures all API routes
func setupRoutes(app *fiber.App) {
	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":   "healthy",
			"database": database,
			"time":     time.Now().UTC().Format(time.RFC3339),
		})
	})

	// API group
	api := app.Group("/")

	// GET /:collection - Find documents with optional filters, sort, projection, pagination
	api.Get("/:collection", handleFind)

	// POST /:collection - Insert one or many documents
	api.Post("/:collection", handleInsert)

	// PUT /:collection - Update documents matching filter
	api.Put("/:collection", handleUpdate)

	// DELETE /:collection - Delete documents matching filter
	api.Delete("/:collection", handleDelete)
}

// handleFind retrieves documents from collection with optional filters
func handleFind(c *fiber.Ctx) error {
	collectionName := c.Params("collection")
	if collectionName == "" {
		return errorResponse(c, fiber.StatusBadRequest, "collection name is required", nil)
	}

	filter := buildFilter(c.Queries())
	limit := parseInt64(c.Query("limit"), defaultLimit)
	if limit > maxLimit {
		limit = maxLimit
	}
	skip := parseInt64(c.Query("skip"), 0)
	sort := parseSort(c.Query("sort"))
	projection := parseProjection(c.Query("fields"))

	findOpts := options.Find().SetLimit(limit).SetSkip(skip)
	if len(sort) > 0 {
		findOpts.SetSort(sort)
	}
	if len(projection) > 0 {
		findOpts.SetProjection(projection)
	}

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	// Async execution with channel
	type findResult struct {
		docs []bson.M
		err  error
	}
	resultChan := make(chan findResult, 1)

	go func() {
		cursor, err := getCollection(collectionName).Find(ctx, filter, findOpts)
		if err != nil {
			resultChan <- findResult{nil, err}
			return
		}
		defer cursor.Close(ctx)

		var docs []bson.M
		if err := cursor.All(ctx, &docs); err != nil {
			resultChan <- findResult{nil, err}
			return
		}

		resultChan <- findResult{docs, nil}
	}()

	select {
	case <-ctx.Done():
		return errorResponse(c, fiber.StatusRequestTimeout, "request timeout", ctx.Err())
	case result := <-resultChan:
		if result.err != nil {
			return errorResponse(c, fiber.StatusInternalServerError, "query failed", result.err)
		}

		return c.JSON(fiber.Map{
			"success": true,
			"count":   len(result.docs),
			"data":    result.docs,
		})
	}
}

// handleInsert inserts one or many documents into collection
func handleInsert(c *fiber.Ctx) error {
	collectionName := c.Params("collection")
	if collectionName == "" {
		return errorResponse(c, fiber.StatusBadRequest, "collection name is required", nil)
	}

	var body interface{}
	if err := c.BodyParser(&body); err != nil {
		return errorResponse(c, fiber.StatusBadRequest, "invalid JSON body", err)
	}

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	// Async execution
	type insertResult struct {
		ids []interface{}
		err error
	}
	resultChan := make(chan insertResult, 1)

	go func() {
		collection := getCollection(collectionName)

		switch data := body.(type) {
		case []interface{}:
			// Insert many
			result, err := collection.InsertMany(ctx, data)
			if err != nil {
				resultChan <- insertResult{nil, err}
				return
			}

			ids := make([]interface{}, len(result.InsertedIDs))
			for i, id := range result.InsertedIDs {
				if oid, ok := id.(primitive.ObjectID); ok {
					ids[i] = oid.Hex()
				} else {
					ids[i] = id
				}
			}
			resultChan <- insertResult{ids, nil}

		case map[string]interface{}:
			// Insert one
			result, err := collection.InsertOne(ctx, data)
			if err != nil {
				resultChan <- insertResult{nil, err}
				return
			}

			id := result.InsertedID
			if oid, ok := id.(primitive.ObjectID); ok {
				id = oid.Hex()
			}
			resultChan <- insertResult{[]interface{}{id}, nil}

		default:
			resultChan <- insertResult{nil, errors.New("body must be JSON object or array")}
		}
	}()

	select {
	case <-ctx.Done():
		return errorResponse(c, fiber.StatusRequestTimeout, "request timeout", ctx.Err())
	case result := <-resultChan:
		if result.err != nil {
			return errorResponse(c, fiber.StatusBadRequest, "insert failed", result.err)
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"success":      true,
			"inserted_ids": result.ids,
		})
	}
}

// handleUpdate updates documents in collection matching filter
func handleUpdate(c *fiber.Ctx) error {
	collectionName := c.Params("collection")
	if collectionName == "" {
		return errorResponse(c, fiber.StatusBadRequest, "collection name is required", nil)
	}

	filter := buildFilter(c.Queries())
	upsert := parseBool(c.Query("upsert"))

	var body map[string]interface{}
	if err := c.BodyParser(&body); err != nil {
		return errorResponse(c, fiber.StatusBadRequest, "invalid JSON body", err)
	}

	// Check if body contains MongoDB update operators, otherwise wrap with $set
	updateDoc := body
	hasOperator := false
	for key := range body {
		if strings.HasPrefix(key, "$") {
			hasOperator = true
			break
		}
	}
	if !hasOperator {
		updateDoc = map[string]interface{}{"$set": body}
	}

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	// Async execution
	type updateResult struct {
		matched  int64
		modified int64
		upserted interface{}
		err      error
	}
	resultChan := make(chan updateResult, 1)

	go func() {
		opts := options.Update().SetUpsert(upsert)
		result, err := getCollection(collectionName).UpdateMany(ctx, filter, updateDoc, opts)
		if err != nil {
			resultChan <- updateResult{0, 0, nil, err}
			return
		}

		var upsertedID interface{}
		if result.UpsertedID != nil {
			if oid, ok := result.UpsertedID.(primitive.ObjectID); ok {
				upsertedID = oid.Hex()
			} else {
				upsertedID = result.UpsertedID
			}
		}

		resultChan <- updateResult{result.MatchedCount, result.ModifiedCount, upsertedID, nil}
	}()

	select {
	case <-ctx.Done():
		return errorResponse(c, fiber.StatusRequestTimeout, "request timeout", ctx.Err())
	case result := <-resultChan:
		if result.err != nil {
			return errorResponse(c, fiber.StatusBadRequest, "update failed", result.err)
		}

		response := fiber.Map{
			"success":  true,
			"matched":  result.matched,
			"modified": result.modified,
		}
		if result.upserted != nil {
			response["upserted_id"] = result.upserted
		}

		return c.JSON(response)
	}
}

// handleDelete deletes documents from collection matching filter
func handleDelete(c *fiber.Ctx) error {
	collectionName := c.Params("collection")
	if collectionName == "" {
		return errorResponse(c, fiber.StatusBadRequest, "collection name is required", nil)
	}

	filter := buildFilter(c.Queries())

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	// Async execution
	type deleteResult struct {
		deleted int64
		err     error
	}
	resultChan := make(chan deleteResult, 1)

	go func() {
		result, err := getCollection(collectionName).DeleteMany(ctx, filter)
		if err != nil {
			resultChan <- deleteResult{0, err}
			return
		}
		resultChan <- deleteResult{result.DeletedCount, nil}
	}()

	select {
	case <-ctx.Done():
		return errorResponse(c, fiber.StatusRequestTimeout, "request timeout", ctx.Err())
	case result := <-resultChan:
		if result.err != nil {
			return errorResponse(c, fiber.StatusBadRequest, "delete failed", result.err)
		}

		return c.JSON(fiber.Map{
			"success": true,
			"deleted": result.deleted,
		})
	}
}

func main() {
	// Load configuration from environment
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("MONGO_URI environment variable is required")
	}

	database = getEnv("MONGO_DBNAME", "metadata")
	port := getEnv("PORT", "8080")
	requestTimeout = time.Duration(parseInt(getEnv("REQUEST_TIMEOUT_MS", "10000"), 10000)) * time.Millisecond
	enableCORS := parseBool(getEnv("ENABLE_CORS", "true"))

	// Setup context for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Connect to MongoDB
	connectTimeout := time.Duration(parseInt(getEnv("MONGO_CONNECT_TIMEOUT_MS", "10000"), 10000)) * time.Millisecond
	connectCtx, connectCancel := context.WithTimeout(ctx, connectTimeout)
	defer connectCancel()

	var err error
	mongoClient, err = connectMongoDB(connectCtx, mongoURI)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	log.Println("✓ Connected to MongoDB")

	// Initialize Fiber app
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ServerHeader:          "Fiber",
		AppName:               "Dynamic MongoDB Metadata API",
	})

	// Middleware
	app.Use(recover.New())
	app.Use(logger.New(logger.Config{
		Format: "[${time}] ${status} - ${latency} ${method} ${path}\n",
	}))

	if enableCORS {
		app.Use(cors.New(cors.Config{
			AllowOrigins: "*",
			AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
			AllowHeaders: "Origin,Content-Type,Accept",
		}))
	}

	// Setup routes
	setupRoutes(app)

	// Start server in goroutine
	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("✓ Server listening on port %s", port)
		serverErrors <- app.Listen(":" + port)
	}()

	// Wait for shutdown signal or server error
	select {
	case err := <-serverErrors:
		log.Fatalf("Server error: %v", err)

	case <-ctx.Done():
		log.Println("Shutting down gracefully...")

		// Graceful shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := app.Shutdown(); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}

		if err := mongoClient.Disconnect(shutdownCtx); err != nil {
			log.Printf("MongoDB disconnect error: %v", err)
		}

		log.Println("✓ Shutdown complete")
	}
}
