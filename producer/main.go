package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type Message struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	TraceID   string    `json:"trace_id"`
	SpanID    string    `json:"span_id"`
	Priority  string    `json:"priority,omitempty"`
	Source    string    `json:"source"`
	Timestamp time.Time `json:"timestamp"`
}

type MessageRequest struct {
	Content  string `json:"content" binding:"required"`
	Priority string `json:"priority,omitempty"`
}

type ElasticLog struct {
	Level     string      `json:"level"`
	Service   string      `json:"service"`
	TraceID   string      `json:"trace_id"`
	SpanID    string      `json:"span_id"`
	Message   string      `json:"message"`
	MessageID string      `json:"message_id,omitempty"`
	Action    string      `json:"action"`
	Error     string      `json:"error,omitempty"`
	Duration  int64       `json:"duration_ms,omitempty"`
	Timestamp time.Time   `json:"@timestamp"`
	Metadata  interface{} `json:"metadata,omitempty"`
}

type Producer struct {
	elasticURL       string
	serviceName      string
	conn             *amqp091.Connection
	channel          *amqp091.Channel
	tracer           oteltrace.Tracer
	messageCounter   metric.Int64Counter
	errorCounter     metric.Int64Counter
	latencyHistogram metric.Float64Histogram
}

type Metrics struct {
	MessagesSent  int64 `json:"messages_sent"`
	MessagesError int64 `json:"messages_error"`
	Uptime        int64 `json:"uptime_seconds"`
}

var startTime = time.Now()

func initTracer() (*trace.TracerProvider, error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://localhost:14268/api/traces"
	}

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerEndpoint)))
	if err != nil {
		return nil, err
	}

	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "producer-service"
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func NewProducer() (*Producer, error) {
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://admin:admin@localhost:5672/"
	}

	elasticURL := os.Getenv("ELASTICSEARCH_URL")
	if elasticURL == "" {
		elasticURL = "http://localhost:9200"
	}

	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "producer-service"
	}

	conn, err := amqp091.Dial(rabbitmqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %v", err)
	}

	// Declare queue with enhanced settings
	_, err = channel.QueueDeclare(
		"message_queue", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		amqp091.Table{
			"x-message-ttl": 3600000, // 1 hour TTL
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	// Initialize metrics
	meter := otel.Meter("producer-service")

	messageCounter, err := meter.Int64Counter(
		"messages_sent_total",
		metric.WithDescription("Total number of messages sent"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create message counter: %v", err)
	}

	errorCounter, err := meter.Int64Counter(
		"messages_error_total",
		metric.WithDescription("Total number of message errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create error counter: %v", err)
	}

	latencyHistogram, err := meter.Float64Histogram(
		"message_send_duration_seconds",
		metric.WithDescription("Message send duration"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create latency histogram: %v", err)
	}

	tracer := otel.Tracer("producer-service")

	return &Producer{
		conn:             conn,
		channel:          channel,
		tracer:           tracer,
		elasticURL:       elasticURL,
		serviceName:      serviceName,
		messageCounter:   messageCounter,
		errorCounter:     errorCounter,
		latencyHistogram: latencyHistogram,
	}, nil
}

func (p *Producer) logToElastic(ctx context.Context, logEntry ElasticLog) {
	go func() {
		jsonData, err := json.Marshal(logEntry)
		if err != nil {
			log.Printf("Failed to marshal log entry: %v", err)
			return
		}

		indexName := fmt.Sprintf("microservice-logs-%s", time.Now().Format("2006.01.02"))
		url := fmt.Sprintf("%s/%s/_doc", p.elasticURL, indexName)

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("Failed to create HTTP request: %v", err)
			return
		}

		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Failed to send log to Elasticsearch: %v", err)
			return
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {

			}
		}(resp.Body)

		if resp.StatusCode >= 400 {
			log.Printf("Elasticsearch returned error status: %d", resp.StatusCode)
		}
	}()
}

func (p *Producer) SendMessage(ctx context.Context, content string, priority string) (*Message, error) {
	startTime := time.Now()

	// Start trace
	ctx, span := p.tracer.Start(ctx, "send_message")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	// Create message
	message := &Message{
		ID:        fmt.Sprintf("msg_%d", time.Now().UnixNano()),
		Content:   content,
		TraceID:   traceID,
		SpanID:    spanID,
		Priority:  priority,
		Source:    p.serviceName,
		Timestamp: time.Now(),
	}

	// Add span attributes
	span.SetAttributes(
		attribute.String("message.id", message.ID),
		attribute.String("message.content", content),
		attribute.String("message.priority", priority),
		attribute.String("queue.name", "message_queue"),
	)

	// Log start of operation
	p.logToElastic(ctx, ElasticLog{
		Level:     "info",
		Message:   "Starting message send operation",
		Action:    "send_start",
		Service:   p.serviceName,
		TraceID:   traceID,
		SpanID:    spanID,
		MessageID: message.ID,
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"content":  content,
			"priority": priority,
		},
	})

	// Serialize message
	messageBytes, err := json.Marshal(message)
	if err != nil {
		span.RecordError(err)
		p.errorCounter.Add(ctx, 1)

		p.logToElastic(ctx, ElasticLog{
			Level:     "error",
			Message:   "Failed to marshal message",
			Action:    "send_error",
			Service:   p.serviceName,
			TraceID:   traceID,
			SpanID:    spanID,
			MessageID: message.ID,
			Error:     err.Error(),
			Timestamp: time.Now(),
		})

		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}

	// Inject trace context into message headers
	headers := make(amqp091.Table)
	carrier := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(carrier))

	for k, v := range carrier {
		headers[k] = v
	}

	// Add priority to headers
	if priority != "" {
		headers["priority"] = priority
	}

	// Send message to RabbitMQ
	err = p.channel.PublishWithContext(
		ctx,
		"",              // exchange
		"message_queue", // routing key
		false,           // mandatory
		false,           // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        messageBytes,
			Headers:     headers,
			Timestamp:   time.Now(),
			Priority:    p.getPriorityLevel(priority),
		})

	duration := time.Since(startTime)

	if err != nil {
		span.RecordError(err)
		p.errorCounter.Add(ctx, 1)

		p.logToElastic(ctx, ElasticLog{
			Level:     "error",
			Message:   "Failed to publish message to RabbitMQ",
			Action:    "send_error",
			Service:   p.serviceName,
			TraceID:   traceID,
			SpanID:    spanID,
			MessageID: message.ID,
			Duration:  duration.Milliseconds(),
			Error:     err.Error(),
			Timestamp: time.Now(),
		})

		return nil, fmt.Errorf("failed to publish message: %v", err)
	}

	// Record success metrics
	span.SetAttributes(attribute.String("status", "sent"))
	p.messageCounter.Add(ctx, 1)
	p.latencyHistogram.Record(ctx, duration.Seconds())

	// Log success
	p.logToElastic(ctx, ElasticLog{
		Timestamp: time.Now(),
		Level:     "info",
		Service:   p.serviceName,
		TraceID:   traceID,
		SpanID:    spanID,
		Message:   "Message sent successfully",
		MessageID: message.ID,
		Action:    "send_success",
		Duration:  duration.Milliseconds(),
		Metadata: map[string]interface{}{
			"queue": "message_queue",
		},
	})

	log.Printf("Message sent: %s (Priority: %s, Duration: %v)", message.ID, priority, duration)

	return message, nil
}

func (p *Producer) getPriorityLevel(priority string) uint8 {
	switch priority {
	case "high":
		return 5
	case "normal":
		return 3
	case "low":
		return 1
	default:
		return 3
	}
}

func (p *Producer) GetMetrics() Metrics {
	return Metrics{
		Uptime: int64(time.Since(startTime).Seconds()),
	}
}

func (p *Producer) Close() {
	if p.channel != nil {
		err := p.channel.Close()
		if err != nil {
			return
		}
	}
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			return
		}
	}
}

func main() {
	// Initialize tracing
	tp, err := initTracer()
	if err != nil {
		log.Fatal("Failed to initialize tracer:", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	// Initialize producer
	producer, err := NewProducer()
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	defer producer.Close()

	// Setup Gin router with middleware
	r := gin.Default()

	// Add request logging middleware
	r.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("%s - [%s] \"%s %s %s %d %s \"%s\" %s\"\n",
			param.ClientIP,
			param.TimeStamp.Format(time.RFC1123),
			param.Method,
			param.Path,
			param.Request.Proto,
			param.StatusCode,
			param.Latency,
			param.Request.UserAgent(),
			param.ErrorMessage,
		)
	}))

	r.Use(gin.Recovery())

	// Health check endpoint with detailed status
	r.GET("/health", func(c *gin.Context) {
		status := gin.H{
			"status":    "healthy",
			"timestamp": time.Now(),
			"service":   producer.serviceName,
			"version":   "1.0.0",
		}

		// Check RabbitMQ connection
		if producer.conn.IsClosed() {
			status["rabbitmq"] = "disconnected"
			c.JSON(http.StatusServiceUnavailable, status)
			return
		}
		status["rabbitmq"] = "connected"

		c.JSON(http.StatusOK, status)
	})

	// Metrics endpoint
	r.GET("/metrics", func(c *gin.Context) {
		metrics := producer.GetMetrics()
		c.JSON(http.StatusOK, metrics)
	})

	// Send message endpoint (enhanced)
	r.POST("/send", func(c *gin.Context) {
		var req MessageRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		priority := req.Priority
		if priority == "" {
			priority = "normal"
		}

		ctx := c.Request.Context()
		message, err := producer.SendMessage(ctx, req.Content, priority)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "Message sent successfully",
			"data":    message,
		})
	})

	// Bulk send endpoint (enhanced)
	r.POST("/send-bulk", func(c *gin.Context) {
		var req struct {
			Content  string `json:"content" binding:"required"`
			Count    int    `json:"count" binding:"required,min=1,max=1000"`
			Priority string `json:"priority,omitempty"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		priority := req.Priority
		if priority == "" {
			priority = "normal"
		}

		ctx := c.Request.Context()
		var messages []*Message
		var errors []string

		for i := 0; i < req.Count; i++ {
			content := fmt.Sprintf("%s - %d", req.Content, i+1)
			message, err := producer.SendMessage(ctx, content, priority)
			if err != nil {
				errors = append(errors, fmt.Sprintf("Message %d: %v", i+1, err))
				continue
			}
			messages = append(messages, message)
		}

		response := gin.H{
			"message":      "Bulk operation completed",
			"sent_count":   len(messages),
			"failed_count": len(errors),
		}

		if len(errors) > 0 {
			response["errors"] = errors
		}

		if len(errors) > len(messages) {
			c.JSON(http.StatusPartialContent, response)
		} else {
			c.JSON(http.StatusOK, response)
		}
	})

	// Simulate load endpoint for testing
	r.POST("/simulate-load", func(c *gin.Context) {
		var req struct {
			Duration int `json:"duration_seconds" binding:"required,min=1,max=1000"`
			Rate     int `json:"messages_per_second" binding:"required,min=1,max=100"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		go func() {
			ticker := time.NewTicker(time.Second / time.Duration(req.Rate))
			timeout := time.After(time.Duration(req.Duration) * time.Second)
			counter := 0

			for {
				select {
				case <-timeout:
					log.Printf("Load simulation completed: sent %d messages", counter)
					return
				case <-ticker.C:
					counter++
					content := fmt.Sprintf("Load test message %d", counter)
					_, err := producer.SendMessage(context.Background(), content, "normal")
					if err != nil {
						return
					}
				}
			}
		}()

		c.JSON(http.StatusAccepted, gin.H{
			"message":  "Load simulation started",
			"duration": req.Duration,
			"rate":     req.Rate,
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8085"
	}

	log.Printf("Producer service starting on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
