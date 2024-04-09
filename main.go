package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
}

var db *sql.DB

func main() {
	var err error
	// Replace "username", "password", and "dbname" with your MySQL credentials and database name
	db, err = sql.Open("mysql", "root:new_password@(127.0.0.1:3306)/")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to the database!")

	// Create the database if it doesn't exist
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS dbname")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Database created successfully!")

	// Switch to the newly created database
	_, err = db.Exec("USE dbname")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Switched to dbname database")

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

	// Routes
	http.HandleFunc("/users", getUsers)
	http.HandleFunc("/user", createUser)
	http.HandleFunc("/user/update", updateUser)
	http.HandleFunc("/user/delete", deleteUser)

	// Start server
	fmt.Println("Server started on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getUsers(w http.ResponseWriter, r *http.Request) {
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
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

	w.WriteHeader(http.StatusOK)
}
