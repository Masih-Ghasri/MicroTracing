package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

type ElasticLog struct {
	Level        string      `json:"level"`
	Service      string      `json:"service"`
	TraceID      string      `json:"trace_id"`
	SpanID       string      `json:"span_id"`
	Message      string      `json:"message"`
	MessageID    string      `json:"message_id,omitempty"`
	Action       string      `json:"action"`
	Error        string      `json:"error,omitempty"`
	Duration     int64       `json:"duration_ms,omitempty"`
	QueueLatency int64       `json:"queue_latency_ms,omitempty"`
	ProcessTime  int64       `json:"process_time_ms,omitempty"`
	Metadata     interface{} `json:"metadata,omitempty"`
	Timestamp    time.Time   `json:"@timestamp"`
}

type ProcessingResult struct {
	MessageID    string
	Success      bool
	Error        error
	Duration     time.Duration
	QueueLatency time.Duration
}

type Consumer struct {
	elasticURL            string
	serviceName           string
	conn                  *amqp091.Connection
	channel               *amqp091.Channel
	tracer                oteltrace.Tracer
	messageCounter        metric.Int64Counter
	errorCounter          metric.Int64Counter
	processingHistogram   metric.Float64Histogram
	queueLatencyHistogram metric.Float64Histogram
	processedCount        int64
	errorCount            int64
	startTime             time.Time
}

type ConsumerStats struct {
	ProcessedMessages int64   `json:"processed_messages"`
	ErrorMessages     int64   `json:"error_messages"`
	Uptime            int64   `json:"uptime_seconds"`
	ProcessingRate    float64 `json:"processing_rate_per_second"`
}

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
		serviceName = "consumer-service"
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

func NewConsumer() (*Consumer, error) {
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
		serviceName = "consumer-service"
	}

	conn, err := amqp091.Dial(rabbitmqURL)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Declare queue (should match producer)
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
		return nil, err
	}

	// Set QoS to control prefetch count
	err = channel.Qos(
		10,    // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, err
	}

	// Initialize metrics
	meter := otel.Meter("consumer-service")

	messageCounter, err := meter.Int64Counter(
		"messages_processed_total",
		metric.WithDescription("Total number of messages processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create message counter: %v", err)
	}

	errorCounter, err := meter.Int64Counter(
		"messages_error_total",
		metric.WithDescription("Total number of message processing errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create error counter: %v", err)
	}

	processingHistogram, err := meter.Float64Histogram(
		"message_processing_duration_seconds",
		metric.WithDescription("Message processing duration"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create processing histogram: %v", err)
	}

	queueLatencyHistogram, err := meter.Float64Histogram(
		"message_queue_latency_seconds",
		metric.WithDescription("Message queue latency"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue latency histogram: %v", err)
	}

	tracer := otel.Tracer("consumer-service")

	return &Consumer{
		conn:                  conn,
		channel:               channel,
		tracer:                tracer,
		elasticURL:            elasticURL,
		serviceName:           serviceName,
		messageCounter:        messageCounter,
		errorCounter:          errorCounter,
		processingHistogram:   processingHistogram,
		queueLatencyHistogram: queueLatencyHistogram,
		startTime:             time.Now(),
	}, nil
}

func (c *Consumer) logToElastic(ctx context.Context, logEntry ElasticLog) {
	go func() {
		jsonData, err := json.Marshal(logEntry)
		if err != nil {
			log.Printf("Failed to marshal log entry: %v", err)
			return
		}

		indexName := fmt.Sprintf("microservice-logs-%s", time.Now().Format("2006.01.02"))
		url := fmt.Sprintf("%s/%s/_doc", c.elasticURL, indexName)

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

func (c *Consumer) processMessage(ctx context.Context, delivery amqp091.Delivery) ProcessingResult {
	startTime := time.Now()
	result := ProcessingResult{
		Success: false,
	}

	// Extract trace context from headers
	carrier := make(map[string]string)
	for key, value := range delivery.Headers {
		if strValue, ok := value.(string); ok {
			carrier[key] = strValue
		}
	}

	// Continue trace from producer
	parentCtx := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
	ctx, span := c.tracer.Start(parentCtx, "process_message")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	// Parse message
	var message Message
	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		result.Error = fmt.Errorf("failed to unmarshal message: %v", err)
		span.RecordError(result.Error)

		c.logToElastic(ctx, ElasticLog{
			Timestamp: time.Now(),
			Level:     "error",
			Service:   c.serviceName,
			TraceID:   traceID,
			SpanID:    spanID,
			Message:   "Failed to parse message",
			Action:    "process_error",
			Error:     result.Error.Error(),
		})
		return result
	}

	result.MessageID = message.ID

	// Calculate queue latency
	queueLatency := time.Since(message.Timestamp)
	result.QueueLatency = queueLatency

	// Add span attributes
	span.SetAttributes(
		attribute.String("message.id", message.ID),
		attribute.String("message.content", message.Content),
		attribute.String("message.priority", message.Priority),
		attribute.String("message.source", message.Source),
		attribute.Int64("queue.latency_ms", queueLatency.Milliseconds()),
	)

	// Log processing start
	c.logToElastic(ctx, ElasticLog{
		Timestamp:    time.Now(),
		Level:        "info",
		Service:      c.serviceName,
		TraceID:      traceID,
		SpanID:       spanID,
		Message:      "Starting message processing",
		MessageID:    message.ID,
		Action:       "process_start",
		QueueLatency: queueLatency.Milliseconds(),
		Metadata: map[string]interface{}{
			"content":           message.Content,
			"priority":          message.Priority,
			"source":            message.Source,
			"original_trace_id": message.TraceID,
		},
	})

	// Simulate processing with different durations based on priority
	processingDelay := c.getProcessingDelay(message.Priority)

	// Add some randomness to simulate real processing
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond
	totalProcessingTime := processingDelay + jitter

	// Simulate processing steps with intermediate logging
	steps := []string{"validate", "transform", "store", "notify"}
	stepDuration := totalProcessingTime / time.Duration(len(steps))

	for i, step := range steps {
		time.Sleep(stepDuration)

		// Log intermediate steps for complex operations
		if message.Priority == "high" || len(message.Content) > 50 {
			c.logToElastic(ctx, ElasticLog{
				Timestamp: time.Now(),
				Level:     "debug",
				Service:   c.serviceName,
				TraceID:   traceID,
				SpanID:    spanID,
				Message:   fmt.Sprintf("Processing step: %s", step),
				MessageID: message.ID,
				Action:    fmt.Sprintf("process_step_%d", i+1),
				Metadata: map[string]interface{}{
					"step":     step,
					"progress": fmt.Sprintf("%d/%d", i+1, len(steps)),
				},
			})
		}

		// Add step as span event
		span.AddEvent(fmt.Sprintf("step_%s_completed", step), oteltrace.WithTimestamp(time.Now()))
	}

	// Simulate occasional failures for testing
	if rand.Float32() < 0.02 { // 2% failure rate
		result.Error = fmt.Errorf("simulated processing error")
		span.RecordError(result.Error)

		c.logToElastic(ctx, ElasticLog{
			Timestamp:    time.Now(),
			Level:        "error",
			Service:      c.serviceName,
			TraceID:      traceID,
			SpanID:       spanID,
			Message:      "Message processing failed",
			MessageID:    message.ID,
			Action:       "process_error",
			QueueLatency: queueLatency.Milliseconds(),
			ProcessTime:  time.Since(startTime).Milliseconds(),
			Error:        result.Error.Error(),
		})
		return result
	}

	result.Success = true
	result.Duration = time.Since(startTime)

	// Record metrics
	c.messageCounter.Add(ctx, 1)
	c.processingHistogram.Record(ctx, result.Duration.Seconds())
	c.queueLatencyHistogram.Record(ctx, queueLatency.Seconds())

	// Add success attributes
	span.SetAttributes(
		attribute.String("status", "processed"),
		attribute.Int64("processing.duration_ms", result.Duration.Milliseconds()),
	)

	// Log successful processing
	c.logToElastic(ctx, ElasticLog{
		Timestamp:    time.Now(),
		Level:        "info",
		Service:      c.serviceName,
		TraceID:      traceID,
		SpanID:       spanID,
		Message:      "Message processed successfully",
		MessageID:    message.ID,
		Action:       "process_success",
		Duration:     result.Duration.Milliseconds(),
		QueueLatency: queueLatency.Milliseconds(),
		ProcessTime:  result.Duration.Milliseconds(),
		Metadata: map[string]interface{}{
			"priority":       message.Priority,
			"content_length": len(message.Content),
		},
	})

	log.Printf("Processed message: %s (Priority: %s, Queue Latency: %v, Process Time: %v)",
		message.ID, message.Priority, queueLatency, result.Duration)

	c.processedCount++
	return result
}

func (c *Consumer) getProcessingDelay(priority string) time.Duration {
	switch priority {
	case "high":
		return time.Duration(100+rand.Intn(200)) * time.Millisecond // 100-300ms
	case "low":
		return time.Duration(500+rand.Intn(1000)) * time.Millisecond // 500-1500ms
	default: // normal
		return time.Duration(200+rand.Intn(300)) * time.Millisecond // 200-500ms
	}
}

func (c *Consumer) StartConsuming(ctx context.Context) error {
	msgs, err := c.channel.Consume(
		"message_queue", // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %v", err)
	}

	log.Println("Consumer started. Waiting for messages...")

	// Log service start
	c.logToElastic(ctx, ElasticLog{
		Timestamp: time.Now(),
		Level:     "info",
		Service:   c.serviceName,
		Message:   "Consumer service started",
		Action:    "service_start",
		Metadata: map[string]interface{}{
			"queue": "message_queue",
		},
	})

	go func() {
		for delivery := range msgs {
			// Create a new context for each message processing
			msgCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

			result := c.processMessage(msgCtx, delivery)

			if result.Success {
				delivery.Ack(false)
			} else {
				c.errorCount++
				c.errorCounter.Add(msgCtx, 1)

				// Log error and decide whether to requeue or reject
				log.Printf("Failed to process message %s: %v", result.MessageID, result.Error)

				// For demonstration, reject messages after 3 attempts
				if delivery.Redelivered {
					log.Printf("Rejecting redelivered message %s", result.MessageID)
					err := delivery.Reject(false)
					if err != nil {
						return
					} // don't requeue
				} else {
					log.Printf("Requeuing message %s for retry", result.MessageID)
					err := delivery.Nack(false, true)
					if err != nil {
						return
					} // requeue for retry
				}
			}

			cancel()
		}
	}()

	return nil
}

func (c *Consumer) GetStats() ConsumerStats {
	uptime := time.Since(c.startTime)
	rate := float64(c.processedCount) / uptime.Seconds()

	return ConsumerStats{
		ProcessedMessages: c.processedCount,
		ErrorMessages:     c.errorCount,
		Uptime:            int64(uptime.Seconds()),
		ProcessingRate:    rate,
	}
}

func (c *Consumer) Close() {
	if c.channel != nil {
		err := c.channel.Close()
		if err != nil {
			return
		}
	}
	if c.conn != nil {
		err := c.conn.Close()
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

	// Initialize consumer
	consumer, err := NewConsumer()
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer consumer.Close()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming messages
	if err := consumer.StartConsuming(ctx); err != nil {
		log.Fatal("Failed to start consuming:", err)
	}

	// Create a simple HTTP server for health checks and metrics
	go func() {
		mux := http.NewServeMux()

		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			status := map[string]interface{}{
				"status":    "healthy",
				"timestamp": time.Now(),
				"service":   consumer.serviceName,
				"version":   "1.0.0",
			}

			// Check RabbitMQ connection
			if consumer.conn.IsClosed() {
				status["rabbitmq"] = "disconnected"
				w.WriteHeader(http.StatusServiceUnavailable)
			} else {
				status["rabbitmq"] = "connected"
				w.WriteHeader(http.StatusOK)
			}

			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(status)
			if err != nil {
				return
			}
		})

		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			stats := consumer.GetStats()
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(stats)
			if err != nil {
				return
			}
		})

		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			stats := consumer.GetStats()
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(stats)
			if err != nil {
				return
			}
		})

		port := os.Getenv("HTTP_PORT")
		if port == "" {
			port = "8086"
		}

		log.Printf("Consumer HTTP server starting on :%s", port)
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigChan
	log.Printf("Received signal %v, shutting down gracefully...", sig)

	// Cancel context to stop consuming
	cancel()

	// Log service shutdown
	consumer.logToElastic(context.Background(), ElasticLog{
		Timestamp: time.Now(),
		Level:     "info",
		Service:   consumer.serviceName,
		Message:   "Consumer service shutting down",
		Action:    "service_stop",
		Metadata: map[string]interface{}{
			"signal":             sig.String(),
			"processed_messages": consumer.processedCount,
			"error_messages":     consumer.errorCount,
			"uptime_seconds":     int64(time.Since(consumer.startTime).Seconds()),
		},
	})

	log.Println("Consumer service stopped")
}
