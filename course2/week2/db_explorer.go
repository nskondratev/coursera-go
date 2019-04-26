package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type apiError struct {
	HTTPStatus int
	Err        error
}

func (ae apiError) Error() string {
	return ae.Err.Error()
}

type pathParams struct {
	Table  *string
	ID     *int
	Limit  int
	Offset int
}

func (pp pathParams) String() string {
	return fmt.Sprintf("pathParams{Table: %v, ID: %v, Limit: %d, Offset: %d}", pp.Table, pp.ID, pp.Limit, pp.Offset)
}

func newPathParams(r *http.Request) (*pathParams, error) {
	pp := &pathParams{Limit: 5, Offset: 0}
	p := strings.Split(r.URL.Path, "/")
	log.Printf("Splitted path: %#v", p)
	for i, pathPart := range p {
		if len(pathPart) < 1 {
			continue
		}
		switch i {
		case 1:
			pp.Table = &pathPart
		case 2:
			id, err := strconv.Atoi(pathPart)
			if err != nil {
				return pp, err
			}
			pp.ID = &id
		}
	}
	rl := r.URL.Query().Get("limit")

	if il, err := strconv.Atoi(rl); len(rl) > 0 && err != nil {
		pp.Limit = il
	}

	ro := r.URL.Query().Get("offset")

	if io, err := strconv.Atoi(ro); len(ro) > 0 && err != nil {
		pp.Offset = io
	}

	return pp, nil
}

func (pp pathParams) isListTables() bool {
	return pp.Table == nil && pp.ID == nil
}

func (pp pathParams) isRecordsList() bool {
	return pp.Table != nil && pp.ID == nil
}

func (pp pathParams) isRecord() bool {
	return pp.Table != nil && pp.ID != nil
}

type responseEnvelope struct {
	Error    string      `json:"error,omitempty"`
	Response interface{} `json:"response,omitempty"`
}

type listTablesResponse struct {
	Tables []string `json:"tables"`
}

type listRecordsResponse struct {
	Records []map[string]interface{} `json:"records"`
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
	db     *sql.DB
	tables map[string]struct {
		Columns []struct {
			Name string
			Type string
		}
	}
}

func NewDbExplorer(db *sql.DB) (*dbExplorer, error) {
	return &dbExplorer{db: db}, nil
}

func (de *dbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] New request on path: %s", r.Method, r.URL.Path)
	pp, err := newPathParams(r)
	if err != nil {
		writeJSON(w, http.StatusNotAcceptable, &responseEnvelope{Error: err.Error()})
	}
	log.Printf("parsed path params: %s", pp)
	switch {
	case pp.isListTables():
		de.HandleGetTablesList(w, r)
	case pp.isRecordsList():
		de.HandleRecordsList(w, r, pp)
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

func (de *dbExplorer) GetRecordsList(table string, limit, offset int) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	currTables, err := de.GetTablesList()
	if err != nil {
		return res, apiError{Err: err, HTTPStatus: http.StatusInternalServerError}
	}
	tableExists := false
	for _, tn := range currTables {
		if table == tn {
			tableExists = true
			break
		}
	}
	if !tableExists {
		return res, apiError{Err: errors.New("unknown table"), HTTPStatus: http.StatusNotFound}
	}
	rows, err := de.db.Query("SELECT * FROM `"+table+"` LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return res, apiError{Err: err, HTTPStatus: http.StatusInternalServerError}
	}
	cols, err := rows.ColumnTypes()
	if err != nil {
		return res, apiError{Err: err, HTTPStatus: http.StatusInternalServerError}
	}
	for rows.Next() {
		values := make([]interface{}, len(cols))

		for i, col := range cols {
			values[i] = reflect.New(col.ScanType()).Interface()
		}

		err := rows.Scan(values...)
		log.Printf("Scanned values: %#v", values)
		if err != nil {
			return res, apiError{Err: err, HTTPStatus: http.StatusInternalServerError}
		}
		toAdd := make(map[string]interface{})

		for i, col := range cols {
			log.Printf("Process column %s, value: %#v", col.Name(), values[i])
			toAdd[col.Name()] = values[i]
		}

		res = append(res, toAdd)
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

func (de *dbExplorer) HandleRecordsList(w http.ResponseWriter, r *http.Request, pp *pathParams) {
	switch r.Method {
	case http.MethodGet:
		res, err := de.GetRecordsList(*pp.Table, pp.Limit, pp.Offset)
		if err != nil {
			c := http.StatusInternalServerError
			if ae, ok := err.(apiError); ok {
				c = ae.HTTPStatus
			}
			writeJSON(w, c, &responseEnvelope{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, &responseEnvelope{Response: &listRecordsResponse{Records: res}})
	default:
		writeJSON(w, http.StatusNotAcceptable, &responseEnvelope{Error: "bad method"})
	}
}
