package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type apiError struct {
	HTTPStatus int
	Err        error
}

func (ae apiError) Error() string {
	return fmt.Sprintf("api error %s", ae.Err.Error())
}

type responseEnvelope struct {
	Error    string      `json:"error,omitempty"`
	Response interface{} `json:"response,omitempty"`
}

type listTablesResponse struct {
	Tables []string `json:"tables"`
}

func writeJSON(w http.ResponseWriter, statusCode int, p interface{}) {
	if statusCode < 0 {
		statusCode = http.StatusInternalServerError
	}
	w.WriteHeader(statusCode)
	err := json.NewEncoder(w).Encode(p)
	if err != nil {
		log.Printf("Error while writing response: %s", err.Error())
	}
}

type dbExplorer struct {
	db *sql.DB
}

func NewDbExplorer(db *sql.DB) (*dbExplorer, error) {
	return &dbExplorer{db: db}, nil
}

func (de *dbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] New request on path: %s", r.Method, r.URL.Path)
	switch r.URL.Path {
	case "/":
		de.HandleGetTablesList(w, r)
	default:
		writeJSON(w, http.StatusNotFound, &responseEnvelope{Error: "unknown path"})
	}
}

func (de *dbExplorer) GetTablesList() ([]string, error) {
	log.Println("GetTablesList method call")
	rows, err := de.db.Query("SHOW TABLES;")
	if err != nil {
		log.Printf("Error while getting tables list: %s", err.Error())
		return nil, apiError{http.StatusInternalServerError, err}
	}
	res := make([]string, 0)
	for rows.Next() {
		var tn string
		err = rows.Scan(&tn)
		if err != nil {
			log.Printf("Error while scanning table name to result: %s", err.Error())
			return nil, apiError{http.StatusInternalServerError, err}
		}
		res = append(res, tn)
	}
	err = rows.Close()
	if err != nil {
		log.Printf("Error while closing rows: %s", err.Error())
		return nil, apiError{http.StatusInternalServerError, err}
	}
	return res, nil
}

func (de *dbExplorer) HandleGetTablesList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusNotAcceptable, &responseEnvelope{Error: "bad method"})
		return
	}
	res, err := de.GetTablesList()
	if err != nil {
		c := http.StatusInternalServerError
		if ae, ok := err.(apiError); ok {
			c = ae.HTTPStatus
		}
		writeJSON(w, c, &responseEnvelope{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, &responseEnvelope{Response: &listTablesResponse{Tables: res}})
}
