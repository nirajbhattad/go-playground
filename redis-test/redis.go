package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
)

type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
}

var (
	db  *sql.DB
	rdb *redis.Client
	ctx = context.Background()
)

func main() {
	var err error

	// Initialize MySQL connection
	db, err = sql.Open("mysql", "root:new_password@(127.0.0.1:3306)/")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Initialize Redis connection
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// Redis connection
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to Redis!")

	// MySQL connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MySQL database!")

	// Create the database if it doesn't exist
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS temporary")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Database created successfully!")

	// Switch to the newly created database
	_, err = db.Exec("USE temporary")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Switched to temporary database")

	// Create table if it doesn't exist
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(50) NOT NULL
    )`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Table created successfully!")

	// Create routes
	http.HandleFunc("/users", getUsers)
	http.HandleFunc("/user", createUser)
	http.HandleFunc("/user/update", updateUser)
	http.HandleFunc("/user/delete", deleteUser)

	// Routes for Redis operations
	http.HandleFunc("/set-string", setString)
	http.HandleFunc("/get-string", getString)
	http.HandleFunc("/set-list", setList)
	http.HandleFunc("/get-list", getList)
	http.HandleFunc("/set-hash", setHash)
	http.HandleFunc("/get-hash", getHash)

	// Start server
	fmt.Println("Server started on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getUsers(w http.ResponseWriter, r *http.Request) {
	// Check if data exists in Redis cache
	usersJSON, err := rdb.Get(ctx, "users").Result()
	if err == nil {
		// If data found in cache, return it
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(usersJSON))
		return
	}

	// If data not found in cache, query MySQL
	rows, err := db.Query("SELECT id, username, email FROM users;")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Username, &user.Email)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		users = append(users, user)
	}

	// Marshal users data to JSON
	usersJSONRes, err := json.Marshal(users)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Set data to Redis cache with expiration time
	err = rdb.Set(ctx, "users", string(usersJSONRes), 2*time.Minute).Err()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return data
	w.Header().Set("Content-Type", "application/json")
	w.Write(usersJSONRes)
}

func createUser(w http.ResponseWriter, r *http.Request) {
	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = db.Exec("INSERT INTO users (username, email) VALUES (?, ?)", user.Username, user.Email)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Update Redis cache
	updateCache()
	w.WriteHeader(http.StatusCreated)
}

func updateUser(w http.ResponseWriter, r *http.Request) {
	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = db.Exec("UPDATE users SET email = ? WHERE username = ?", user.Email, user.Username)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Update Redis cache
	updateCache()

	w.WriteHeader(http.StatusOK)
}

func deleteUser(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "Missing username parameter", http.StatusBadRequest)
		return
	}

	_, err := db.Exec("DELETE FROM users WHERE username = ?", username)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Update Redis cache
	updateCache()

	w.WriteHeader(http.StatusOK)
}

func updateCache() {
	// Query MySQL
	rows, err := db.Query("SELECT id, username, email FROM users;")
	if err != nil {
		log.Println("Failed to query MySQL:", err)
		return
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Username, &user.Email)
		if err != nil {
			log.Println("Failed to scan row:", err)
			return
		}
		users = append(users, user)
	}

	// Marshal users data to JSON
	usersJSON, err := json.Marshal(users)
	if err != nil {
		log.Println("Failed to marshal JSON:", err)
		return
	}

	// Set data to Redis cache with expiration time
	err = rdb.Set(ctx, "users", usersJSON, 5*time.Minute).Err()
	if err != nil {
		log.Println("Failed to update Redis cache:", err)
		return
	}
}

// Redis Functions
func setString(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w, "Missing key or value parameters", http.StatusBadRequest)
		return
	}

	err := rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getString(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Value for key %s: %s\n", key, val)
}

func setList(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	values := r.URL.Query()["value"]
	if key == "" || len(values) == 0 {
		http.Error(w, "Missing key or value parameters", http.StatusBadRequest)
		return
	}

	err := rdb.RPush(ctx, key, values).Err()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getList(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	vals, err := rdb.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Values for key %s: %v\n", key, vals)
}

func setHash(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	field := r.URL.Query().Get("field")
	value := r.URL.Query().Get("value")
	if key == "" || field == "" || value == "" {
		http.Error(w, "Missing key, field, or value parameters", http.StatusBadRequest)
		return
	}

	err := rdb.HSet(ctx, key, field, value).Err()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getHash(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	field := r.URL.Query().Get("field")
	if key == "" || field == "" {
		http.Error(w, "Missing key or field parameter", http.StatusBadRequest)
		return
	}

	val, err := rdb.HGet(ctx, key, field).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Value for field %s in key %s: %s\n", field, key, val)
}
