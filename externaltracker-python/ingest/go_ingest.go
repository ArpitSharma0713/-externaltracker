package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type ValidatedJobPayload struct {
	Title           string `json:"title"`
	CompanySlug     string `json:"company_slug"`
	JobSourceURL    string `json:"job_source_url"`
	RawDescription  string `json:"raw_description"`
	RawLocationText string `json:"raw_location_text"`
	PostedTimestamp string `json:"posted_timestamp"`
	TechTrack       string `json:"tech_track"`
	Source          string `json:"source"`
}

type SignedEnvelope struct {
	Job       ValidatedJobPayload `json:"job"`
	Signature string              `json:"signature"`
}

var ctx = context.Background()

func loadDotEnv(
	path string,
) {
	file, err := os.Open(path)

	if err != nil {
		return
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(
			scanner.Text(),
		)

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(
			line,
			"=",
		)

		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		if key == "" || os.Getenv(key) != "" {
			continue
		}

		value = strings.Trim(
			value,
			`"'`,
		)

		os.Setenv(
			key,
			value,
		)
	}
}

func getEnv(
	key string,
	fallback string,
) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}

func getEnvInt(
	key string,
	fallback int,
) int {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	parsedValue, err := strconv.Atoi(value)

	if err != nil {
		return fallback
	}

	return parsedValue
}

func verifyHMAC(
	job ValidatedJobPayload,
	signature string,
	secret string,
) bool {
	if secret == "" {
		return false
	}

	if signature == "" {
		return false
	}

	body, err := json.Marshal(job)

	if err != nil {
		return false
	}

	mac := hmac.New(
		sha256.New,
		[]byte(secret),
	)

	mac.Write(body)

	expectedSignature := hex.EncodeToString(
		mac.Sum(nil),
	)

	return hmac.Equal(
		[]byte(expectedSignature),
		[]byte(signature),
	)
}

func parseCleanJob(
	rawMessage string,
	allowUnsignedLocalJobs bool,
	hmacSecret string,
) (
	ValidatedJobPayload,
	bool,
) {
	var envelope SignedEnvelope

	envelopeErr := json.Unmarshal(
		[]byte(rawMessage),
		&envelope,
	)

	if envelopeErr == nil && envelope.Job.Title != "" {
		if envelope.Job.Source == "" {
			envelope.Job.Source = "social"
		}

		if allowUnsignedLocalJobs {
			return envelope.Job, true
		}

		if verifyHMAC(
			envelope.Job,
			envelope.Signature,
			hmacSecret,
		) {
			return envelope.Job, true
		}

		fmt.Println(
			"[GO-CORE] HMAC verification failed. Dropping job.",
		)

		return ValidatedJobPayload{}, false
	}

	var directJob ValidatedJobPayload

	directErr := json.Unmarshal(
		[]byte(rawMessage),
		&directJob,
	)

	if directErr == nil && directJob.Title != "" {
		if directJob.Source == "" {
			directJob.Source = "social"
		}

		if allowUnsignedLocalJobs {
			return directJob, true
		}

		fmt.Println(
			"[GO-CORE] Direct unsigned job rejected. Enable ALLOW_UNSIGNED_LOCAL_JOBS=true for local testing.",
		)

		return ValidatedJobPayload{}, false
	}

	fmt.Println(
		"[GO-CORE] Invalid clean job JSON. Dropping message.",
	)

	return ValidatedJobPayload{}, false
}

func executeIsolatedSQLTransaction(
	dbConn *pgx.Conn,
	jobs []ValidatedJobPayload,
) error {
	tx, err := dbConn.Begin(
		ctx,
	)

	if err != nil {
		return err
	}

	defer tx.Rollback(
		ctx,
	)

	insertedCount := 0

	for _, job := range jobs {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO social_jobs (
				title,
				company_slug,
				job_source_url,
				raw_description,
				raw_location_text,
				posted_timestamp,
				tech_track,
				source
			)
			VALUES (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6,
				$7,
				$8
			)
			ON CONFLICT (job_source_url) DO NOTHING;
			`,
			job.Title,
			job.CompanySlug,
			job.JobSourceURL,
			job.RawDescription,
			job.RawLocationText,
			job.PostedTimestamp,
			job.TechTrack,
			job.Source,
		)

		if err != nil {
			return err
		}

		insertedCount++
	}

	err = tx.Commit(
		ctx,
	)

	if err != nil {
		return err
	}

	fmt.Printf(
		"[DB-SUCCESS] Inserted/ignored batch of %d jobs into social_jobs.\n",
		insertedCount,
	)

	return nil
}

func startIngestionEngineFunnel(
	rdb *redis.Client,
	dbConn *pgx.Conn,
	maxBatchSize int,
	cooldownJitter time.Duration,
	allowUnsignedLocalJobs bool,
	hmacSecret string,
) {
	fmt.Printf(
		"[GO-CORE] Ingestion Funnel active. BATCH_SIZE=%d\n",
		maxBatchSize,
	)

	fmt.Printf(
		"[GO-CORE] ALLOW_UNSIGNED_LOCAL_JOBS=%v\n",
		allowUnsignedLocalJobs,
	)

	var processingBatchPool []ValidatedJobPayload

	for {
		result, err := rdb.BLPop(
			ctx,
			0,
			"provio_clean_jobs_staging",
		).Result()

		if err != nil {
			fmt.Println(
				"[GO-CORE] Redis BLPOP error:",
				err,
			)

			continue
		}

		if len(result) < 2 {
			continue
		}

		rawMessage := result[1]

		cleanJob, ok := parseCleanJob(
			rawMessage,
			allowUnsignedLocalJobs,
			hmacSecret,
		)

		if !ok {
			continue
		}

		processingBatchPool = append(
			processingBatchPool,
			cleanJob,
		)

		fmt.Printf(
			"[GO-CORE] Queued clean job: %s @ %s\n",
			cleanJob.Title,
			cleanJob.CompanySlug,
		)

		if len(processingBatchPool) >= maxBatchSize {
			err := executeIsolatedSQLTransaction(
				dbConn,
				processingBatchPool,
			)

			if err != nil {
				fmt.Println(
					"[CRITICAL-DB-ABORT] Re-buffering jobs:",
					err,
				)

				for _, failedJob := range processingBatchPool {
					marshaledJob, marshalErr := json.Marshal(
						failedJob,
					)

					if marshalErr != nil {
						continue
					}

					rdb.RPush(
						ctx,
						"provio_clean_jobs_staging",
						string(marshaledJob),
					)
				}
			}

			processingBatchPool = nil

			time.Sleep(
				cooldownJitter,
			)
		}
	}
}

func main() {
	loadDotEnv(
		"../.env",
	)

	redisHost := getEnv(
		"REDIS_HOST",
		"localhost",
	)

	redisPort := getEnv(
		"REDIS_PORT",
		"6379",
	)

	databaseURL := getEnv(
		"DATABASE_URL",
		"",
	)

	if databaseURL == "" {
		fmt.Println(
			"[GO-CORE] DATABASE_URL is missing.",
		)

		os.Exit(
			1,
		)
	}

	hmacSecret := getEnv(
		"PROVIO_INGEST_HMAC_SECRET",
		"",
	)

	allowUnsignedLocalJobs := getEnv(
		"ALLOW_UNSIGNED_LOCAL_JOBS",
		"true",
	) == "true"

	maxBatchSize := getEnvInt(
		"GO_INGEST_BATCH_SIZE",
		1,
	)

	redisAddress := fmt.Sprintf(
		"%s:%s",
		redisHost,
		redisPort,
	)

	rdb := redis.NewClient(
		&redis.Options{
			Addr: redisAddress,
		},
	)

	_, err := rdb.Ping(
		ctx,
	).Result()

	if err != nil {
		fmt.Println(
			"[GO-CORE] Redis connection failed:",
			err,
		)

		os.Exit(
			1,
		)
	}

	fmt.Println(
		"[GO-CORE] Connected to Redis.",
	)

	dbConn, err := pgx.Connect(
		ctx,
		databaseURL,
	)

	if err != nil {
		fmt.Println(
			"[GO-CORE] PostgreSQL connection failed:",
			err,
		)

		os.Exit(
			1,
		)
	}

	defer dbConn.Close(
		ctx,
	)

	fmt.Println(
		"[GO-CORE] Connected to PostgreSQL.",
	)

	startIngestionEngineFunnel(
		rdb,
		dbConn,
		maxBatchSize,
		150*time.Millisecond,
		allowUnsignedLocalJobs,
		hmacSecret,
	)
}
