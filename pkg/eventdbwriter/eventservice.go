package eventdbwriter

import (
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func StartEventService(dbInfo DBInfo, yunikornHost string) {
	storage := createStorage(dbInfo)

	GetLogger().Info("Starting event service")
	GetLogger().Info("Yunikorn host", zap.String("host", yunikornHost))
	GetLogger().Info("Database properties", zap.String("host", dbInfo.Host),
		zap.Int("port", dbInfo.Port), zap.String("db name", dbInfo.Name),
		zap.String("username", dbInfo.User))

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
		GetLogger().Fatal("Could not create DB session", zap.Error(err))
		return nil
	}
	s.db = db
	err = db.Migrator().AutoMigrate(&EventDBEntry{})
	if err != nil {
		GetLogger().Fatal("DB auto migration failed", zap.Error(err))
		return nil
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
