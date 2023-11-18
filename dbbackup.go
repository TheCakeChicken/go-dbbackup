package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"os"
	"os/exec"
	"os/signal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/robfig/cron"
	"gopkg.in/yaml.v3"
)

// Hold the individual database configurations
type DatabaseConfig struct {
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	DBName   string   `yaml:"name"`
	DBNames  []string `yaml:"names"`
}

// Hold the configuration for the entire application
type Config struct {
	CronInterval string `yaml:"cron_interval"`
	HeartbeatUri string `yaml:"heartbeat_uri"`

	S3Config struct {
		AccessKey    string `yaml:"access_key"`
		AccessSecret string `yaml:"access_secret"`
		Region       string `yaml:"region"`
		Bucket       string `yaml:"bucket"`
	} `yaml:"s3_config"`

	Databases []DatabaseConfig `yaml:"databases"`
}

// File compression functions (https://www.arthurkoziel.com/writing-tar-gz-files-in-go/)
func createArchive(files []string, buf io.Writer) error {
	// Create new Writers for gzip and tar
	// These writers are chained. Writing to the tar writer will
	// write to the gzip writer which in turn will write to
	// the "buf" writer
	gw := gzip.NewWriter(buf)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Iterate over files and add them to the tar archive
	for _, file := range files {
		err := addToArchive(tw, file)
		if err != nil {
			return err
		}
	}

	return nil
}

func addToArchive(tw *tar.Writer, filename string) error {
	// Open the file which will be written into the archive
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get FileInfo about our file providing file size, mode, etc.
	info, err := file.Stat()
	if err != nil {
		return err
	}

	// Create a tar Header from the FileInfo data
	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return err
	}

	// Use full path as name (FileInfoHeader only takes the basename)
	// If we don't do this the directory strucuture would
	// not be preserved
	// https://golang.org/src/archive/tar/common.go?#L626
	header.Name = filename

	// Write file header to the tar archive
	err = tw.WriteHeader(header)
	if err != nil {
		return err
	}

	// Copy file content to tar archive
	_, err = io.Copy(tw, file)
	if err != nil {
		return err
	}

	return nil
}

// Entrypoint
func main() {
	// Check if mysqldump is installed
	cmd := exec.Command("mysqldump", "--help")
	_, err := cmd.Output()

	if err != nil {
		log.Fatalf("Error running mysqldump: %s\n", err.Error())
		return
	}

	// Load the configuration file
	log.Println("Loading configuration file...")
	config := Config{}

	configFile, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Error reading configuration file: %s\n", err.Error())
		return
	}

	// Parse the configuration file
	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatalf("Error parsing configuration file: %s\n", err.Error())
		return
	}

	// Create the backup directory if it doesn't exist
	if _, err := os.Stat("backups"); os.IsNotExist(err) {
		log.Printf("Backup directory not found! Creating backup directory.\n")
		os.Mkdir("backups", 0755)
	}

	// Create the temp directory if it doesn't exist
	if _, err := os.Stat("temp"); os.IsNotExist(err) {
		log.Printf("Temp directory not found! Creating temp directory.\n")
		os.Mkdir("temp", 0755)
	}

	if len(os.Args) > 1 {
		if (os.Args[1] == "--test") || (os.Args[1] == "-t") {
			log.Println("Running backup job to test configuration")
			runBackups(config)
			return
		} else {
			log.Println("Unrecognised argument(s)")
			return
		}
	}
	// Create the cron job to run backups at the specified interval
	log.Println("Starting cronjob to run backups")

	c := cron.New()
	c.AddFunc(config.CronInterval, func() {
		runBackups(config)
	})
	go c.Start()

	// Wait for signal to exit
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill)
	<-sig
}

func runBackups(config Config) {
	log.Println("Starting backup jobs")

	backupStartTimestamp := time.Now().Format("2006-01-02_15-04-05")

	// Delete the files in the temp directory
	log.Println("Deleting temp files")

	err := os.Remove("./temp/backup.tar.gz")
	if err != nil {
		log.Printf("Error deleting file %s: %s\n", "./temp/backup.tar.gz", err.Error())
	}

	// Loop through each database and run a backup
	files := []string{}

	for _, db := range config.Databases {
		if db.DBName != "" {
			db.DBNames = append(db.DBNames, db.DBName)
		}

		for _, dbName := range db.DBNames {
			log.Printf("Backing up database %s on host %s\n", dbName, db.Host)

			backupTime := time.Now().Format("2006-01-02_15-04-05")

			exportName := fmt.Sprintf("%s_%s_%s", backupTime, db.Host, dbName)

			if dbName == "*" {
				dbName = "--all-databases"
				exportName = fmt.Sprintf("%s_%s_all-databases", backupTime, db.Host)
			}

			hostArg := fmt.Sprintf("--host=%s", db.Host)
			portArg := fmt.Sprintf("--port=%d", db.Port)
			usernameArg := fmt.Sprintf("--user=%s", db.Username)
			passwordArg := fmt.Sprintf("--password=%s", db.Password)
			outputArg := fmt.Sprintf("--result-file=./backups/%s.sql", exportName)

			files = append(files, fmt.Sprintf("backups/%s.sql", exportName))

			// TODO: Check if --column-statistics=0 is needed (Needed on MySQL 8.0.17+, flag not available in MariaDB mysqldump)
			cmd := exec.Command("mysqldump", hostArg, portArg, usernameArg, passwordArg, outputArg, "--extended-insert", "--single-transaction=TRUE", dbName)
			_, err := cmd.Output()

			if err != nil {
				log.Printf("Error running backup: %s\n", err.Error())
				continue
			}
		}
	}

	// Tar and gzip the backup directory
	log.Println("Compressing backup files")

	// Create output file
	out, err := os.Create("./temp/backup.tar.gz")
	if err != nil {
		log.Fatalln("Error writing archive:", err)
	}
	defer out.Close()

	// Create the archive and write the output to the "out" Writer
	err = createArchive(files, out)
	if err != nil {
		log.Fatalln("Error creating archive:", err)
	}

	log.Println("Compressed backup files")

	// Upload to S3
	log.Println("Uploading to S3")

	// Create S3 client
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(config.S3Config.AccessKey, config.S3Config.AccessSecret, ""),
		Region:      aws.String(config.S3Config.Region),
	})

	if err != nil {
		log.Fatalf("Error creating S3 session: %s\n", err.Error())
		return
	}

	uploader := s3manager.NewUploader(sess)

	// Open the file for use
	file, err := os.Open("./temp/backup.tar.gz")
	if err != nil {
		log.Fatalf("Error opening file %s: %s\n", "./temp/backup.tar.gz", err.Error())
		return
	}
	defer file.Close()

	// Upload the file to S3
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(config.S3Config.Bucket),
		Key:    aws.String(fmt.Sprintf("sql_backup_at_%s.tar.gz", backupStartTimestamp)),
		Body:   file,
	})

	if err != nil {
		log.Fatalf("Error uploading file to S3: %s\n", err.Error())
		return
	}

	log.Println("Successfully uploaded backup to S3")

	// Delete the files in the backup directory
	log.Println("Deleting backup files")

	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			log.Printf("Error deleting file %s: %s\n", file, err.Error())
		}
	}

	// Make a HTTP request to the heartbeat URI to let the server know we're still alive
	if config.HeartbeatUri != "" {
		log.Println("Sending heartbeat")
		http.Get(config.HeartbeatUri)
	}
}
