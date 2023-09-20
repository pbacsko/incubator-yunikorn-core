package eventdbwriter

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func StartEventService(dbInfo DBInfo, yunikornHost string) {
	storage := createStorage(dbInfo)

	log.Println("Starting event service")
	log.Printf("Yunikorn host: %s", yunikornHost)
	log.Printf("Database host: %s, port: %d, name: %s, username: %s", dbInfo.Host,
		dbInfo.Port, dbInfo.Name, dbInfo.User)

	cache := NewEventCache()
	webservice := NewWebService(cache, storage)
	writer := NewEventWriter(storage, NewHttpClient(yunikornHost), cache)

	cache.Start()
	webservice.Start()
	writer.Start()
}

func createStorage(dbInfo DBInfo) Storage {
	s := &DBStorage{}
	db, err := openDBSession(dbInfo)
	if err != nil {
		log.Fatalf("Could not create DB session: %v", err)
		return nil
	}
	s.db = db
	err = db.Migrator().AutoMigrate(&EventDBEntry{})
	if err != nil {
		log.Fatalf("DB auto migration failed: %v", err)
	}

	return s
}

func openDBSession(dbInfo DBInfo) (*gorm.DB, error) {
	dsn := getConnectionString(dbInfo)
	var db *gorm.DB
	// set same nanosecond timestamp as used in postgres for time
	var datetimePrecision = 6
	cfg := mysql.Config{
		DSN:                      dsn,
		DefaultDatetimePrecision: &datetimePrecision,
	}
	var err error
	if db, err = gorm.Open(mysql.New(cfg)); err != nil {
		return nil, fmt.Errorf("error creating DB session for '%s', error: %w", "mysql", err)
	}

	var sqlDB *sql.DB
	sqlDB, err = db.DB()
	if err != nil {
		return nil, fmt.Errorf("error getting DB connection pool, err: %w", err)
	}
	sqlDB.SetMaxIdleConns(25)
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)
	sqlDB.SetConnMaxLifetime(time.Hour)
	return db, nil
}

func getConnectionString(dbInfo DBInfo) string {
	return fmt.Sprintf("%s:%s@%s(%s:%d)/%s?%s=%s&%s=%s&%s=%s&%s=%s",
		dbInfo.User,
		dbInfo.Password,
		"tcp",
		dbInfo.Host,
		dbInfo.Port,
		dbInfo.Name,
		"collation", "utf8mb4_general_ci",
		"interpolateParams", "true",
		"parseTime", "true",
		"loc", "UTC")
}
