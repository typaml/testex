package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "root"
	password = "1234s"
	dbname   = "admindb"
)

// Wallet представляет состояние кошелька
type Wallet struct {
	ID      string  `json:"id"`
	Balance float64 `json:"balance"`
}

// Transaction представляет информацию о транзакции
type Transaction struct {
	Time   time.Time `json:"time"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	Amount float64   `json:"amount"`
}

type DBStore struct {
	db *sql.DB
}

// NewDBStore создает новый экземпляр DBStore
func NewDBStore(db *sql.DB) *DBStore {
	return &DBStore{
		db: db,
	}
}

// CreateWallet создает новый кошелек в базе данных
func (s *DBStore) CreateWallet() (*Wallet, error) {
	id := uuid.New().String()
	balance := 100.0

	_, err := s.db.Exec("INSERT INTO wallets (id, balance) VALUES ($1, $2)", id, balance)
	if err != nil {
		return nil, err
	}

	return &Wallet{
		ID:      id,
		Balance: balance,
	}, nil
}

// GetWallet возвращает кошелек из базы данных по его ID
func (s *DBStore) GetWallet(walletID string) (*Wallet, error) {
	var wallet Wallet
	err := s.db.QueryRow("SELECT id, balance FROM wallets WHERE id = $1", walletID).Scan(&wallet.ID, &wallet.Balance)
	if err != nil {
		return nil, err
	}
	return &wallet, nil
}

// Transfer осуществляет перевод средств между кошельками в базе данных
func (s *DBStore) Transfer(fromID, toID string, amount float64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Проверка баланса отправителя
	var fromBalance float64
	err = tx.QueryRow("SELECT balance FROM wallets WHERE id = $1 FOR UPDATE", fromID).Scan(&fromBalance)
	if err != nil {
		return err
	}

	if fromBalance < amount {
		return fmt.Errorf("insufficient funds")
	}

	// Обновление баланса отправителя
	_, err = tx.Exec("UPDATE wallets SET balance = balance - $1 WHERE id = $2", amount, fromID)
	if err != nil {
		return err
	}

	// Обновление баланса получателя
	_, err = tx.Exec("UPDATE wallets SET balance = balance + $1 WHERE id = $2", amount, toID)
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO transactions (from_wallet, to_wallet, amount) VALUES ($1, $2, $3)", fromID, toID, amount)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// GetHistory возвращает историю транзакций для указанного кошелька из базы данных
func (s *DBStore) GetHistory(walletID string) ([]Transaction, error) {
	rows, err := s.db.Query("SELECT time, from_wallet, to_wallet, amount FROM transactions WHERE from_wallet = $1 OR to_wallet = $1", walletID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []Transaction
	for rows.Next() {
		var transaction Transaction
		err := rows.Scan(&transaction.Time, &transaction.From, &transaction.To, &transaction.Amount)
		if err != nil {
			return nil, err
		}
		history = append(history, transaction)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return history, nil
}

type HTTPHandler struct {
	store *DBStore
}

func NewHTTPHandler(store *DBStore) *HTTPHandler {
	return &HTTPHandler{
		store: store,
	}
}

// CreateWalletHandler обрабатывает запрос на создание нового кошелька
func (h *HTTPHandler) CreateWalletHandler(w http.ResponseWriter, r *http.Request) {
	wallet, err := h.store.CreateWallet()
	if err != nil {
		responseJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create wallet"})
		return
	}
	responseJSON(w, http.StatusOK, wallet)
}

// TransferHandler обрабатывает запрос на перевод средств между кошельками
func (h *HTTPHandler) TransferHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fromID := vars["walletId"]

	var request struct {
		To     string  `json:"to"`
		Amount float64 `json:"amount"`
	}

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		responseJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	err = h.store.Transfer(fromID, request.To, request.Amount)
	if err != nil {
		responseJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	responseJSON(w, http.StatusOK, map[string]string{"message": "transfer successful"})
}

// GetHistoryHandler обрабатывает запрос на получение истории транзакций для указанного кошелька
func (h *HTTPHandler) GetHistoryHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	walletID := vars["walletId"]

	history, err := h.store.GetHistory(walletID)
	if err != nil {
		responseJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	responseJSON(w, http.StatusOK, history)
}

// GetWalletHandler обрабатывает запрос на получение текущего состояния кошелька
func (h *HTTPHandler) GetWalletHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	walletID := vars["walletId"]

	wallet, err := h.store.GetWallet(walletID)
	if err != nil {
		responseJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	responseJSON(w, http.StatusOK, wallet)
}

func responseJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func main() {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	store := NewDBStore(db)
	handler := NewHTTPHandler(store)

	//маршруты
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/wallet", handler.CreateWalletHandler).Methods("POST")
	r.HandleFunc("/api/v1/wallet/{walletId}/send", handler.TransferHandler).Methods("POST")
	r.HandleFunc("/api/v1/wallet/{walletId}/history", handler.GetHistoryHandler).Methods("GET")
	r.HandleFunc("/api/v1/wallet/{walletId}", handler.GetWalletHandler).Methods("GET")

	port := 8080
	fmt.Printf("Server is listening on :%d...\n", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), r)
}
