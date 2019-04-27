package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
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
		pathPart := pathPart
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

	if il, err := strconv.Atoi(rl); len(rl) > 0 && err == nil {
		pp.Limit = il
	}

	ro := r.URL.Query().Get("offset")

	if io, err := strconv.Atoi(ro); len(ro) > 0 && err == nil {
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

type recordResponse struct {
	Record interface{} `json:"record"`
}

type idResponse struct {
	ID int `json:"id"`
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

type nullString struct {
	sql.NullString
}

func (ns *nullString) MarshalJSON() ([]byte, error) {
	if !ns.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ns.String)
}

type columnDef struct {
	Name       string
	Type       string
	Collation  sql.RawBytes
	Null       string
	Key        string
	Default    sql.RawBytes
	Extra      string
	Privileges string
	Comment    string
}

func (c columnDef) nullable() bool {
	return c.Null == "YES"
}

func (c columnDef) isPK() bool {
	return c.Key == "PRI"
}

func (c columnDef) New() interface{} {
	t := strings.ToLower(c.Type)
	switch {
	case strings.Contains(t, "int"):
		return new(int)
	case strings.Contains(t, "char") && !c.nullable(), strings.Contains(t, "text") && !c.nullable():
		return new(string)
	case strings.Contains(t, "char") && c.nullable(), strings.Contains(t, "text") && c.nullable():
		return new(nullString)
	default:
		return new(sql.RawBytes)
	}
}

type dbExplorer struct {
	db      *sql.DB
	tables  []string
	columns map[string][]*columnDef
}

func NewDbExplorer(db *sql.DB) (*dbExplorer, error) {
	// Fetch tables
	rows, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}
	tables := make([]string, 0)
	for rows.Next() {
		var tn string
		err = rows.Scan(&tn)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tn)
	}
	log.Printf("Tables: %#v", tables)
	if err := rows.Close(); err != nil {
		return nil, err
	}
	// Fetch columns for each table
	columns := make(map[string][]*columnDef)
	for _, table := range tables {
		cols, err := db.Query("SHOW FULL COLUMNS FROM `" + table + "`;")
		if err != nil {
			return nil, err
		}
		curColumns := make([]*columnDef, 0)
		for cols.Next() {
			colDef := &columnDef{}
			if err := cols.Scan(&colDef.Name, &colDef.Type, &colDef.Collation, &colDef.Null, &colDef.Key, &colDef.Default, &colDef.Extra, &colDef.Privileges, &colDef.Comment); err != nil {
				return nil, err
			}
			curColumns = append(curColumns, colDef)
		}
		if err := cols.Close(); err != nil {
			return nil, err
		}
		columns[table] = curColumns
	}
	return &dbExplorer{db: db, tables: tables, columns: columns}, nil
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
	case pp.isRecord():
		de.HandleRecord(w, r, pp)
	default:
		writeJSON(w, http.StatusNotFound, &responseEnvelope{Error: "unknown path"})
	}
}

func (de *dbExplorer) GetTablesList() ([]string, error) {
	log.Println("GetTablesList method call")
	res := de.tables
	return res, nil
}

func (de *dbExplorer) GetRecordsList(table string, limit, offset int) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	tableExists := false
	for _, tn := range de.tables {
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
	cols := de.columns[table]
	for rows.Next() {
		values := make([]interface{}, len(cols))

		for i, col := range cols {
			values[i] = col.New()
		}

		err := rows.Scan(values...)
		if err != nil {
			return res, apiError{Err: err, HTTPStatus: http.StatusInternalServerError}
		}
		toAdd := make(map[string]interface{})

		for i, col := range cols {
			toAdd[col.Name] = values[i]
		}

		res = append(res, toAdd)
	}

	return res, nil
}

func (de *dbExplorer) GetRecordById(table string, id int) (map[string]interface{}, error) {
	log.Printf("GetRecordById call. Table: %s, id: %d", table, id)
	tableExists := false
	for _, tn := range de.tables {
		if table == tn {
			tableExists = true
			break
		}
	}
	if !tableExists {
		return nil, apiError{Err: errors.New("unknown table"), HTTPStatus: http.StatusNotFound}
	}

	cols := de.columns[table]
	values := make([]interface{}, len(cols))

	for i, col := range cols {
		values[i] = col.New()
	}

	row := de.db.QueryRow("SELECT * FROM `"+table+"` WHERE id = ?", id)
	err := row.Scan(values...)
	if err == sql.ErrNoRows {
		return nil, apiError{Err: errors.New("record not found"), HTTPStatus: http.StatusNotFound}
	} else if err != nil {
		return nil, apiError{Err: err, HTTPStatus: http.StatusInternalServerError}
	}
	res := make(map[string]interface{})
	for i, col := range cols {
		res[col.Name] = values[i]
	}
	return res, nil
}

func (de *dbExplorer) CreateRecord(table string, record map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("CreateRecord method call. table: %s, record: %#v", table, record)
	tableExists := false
	for _, tn := range de.tables {
		if table == tn {
			tableExists = true
			break
		}
	}
	if !tableExists {
		return nil, apiError{Err: errors.New("unknown table"), HTTPStatus: http.StatusNotFound}
	}

	cols := make([]*columnDef, 0)

	var pkColName string
	for _, col := range de.columns[table] {
		if !col.isPK() {
			cols = append(cols, col)
		} else {
			pkColName = col.Name
		}
	}

	values := make([]interface{}, len(cols))
	colNames := make([]string, len(cols), len(cols))
	placeholders := make([]string, len(cols), len(cols))

	for i, col := range cols {
		if v, ok := record[col.Name]; ok {
			values[i] = v
		} else {
			values[i] = col.Default
		}
		colNames[i] = col.Name
		placeholders[i] = "?"
	}

	res, err := de.db.Exec("INSERT INTO `"+table+"` (`"+strings.Join(colNames, "`,`")+"`) VALUES ("+strings.Join(placeholders, ",")+")", values...)
	if err != nil {
		return nil, apiError{HTTPStatus: http.StatusInternalServerError, Err: err}
	}
	id, err := res.LastInsertId()

	if err != nil {
		return nil, apiError{HTTPStatus: http.StatusInternalServerError, Err: err}
	}
	log.Printf("pkColName: %s", pkColName)
	return map[string]interface{}{pkColName: id}, nil
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
	case http.MethodPut:
		decoder := json.NewDecoder(r.Body)
		rb := make(map[string]interface{})
		err := decoder.Decode(&rb)
		if err != nil {
			log.Printf("Error while parsing json request body: %s", err.Error())
			writeJSON(w, http.StatusNotAcceptable, &responseEnvelope{Error: "bad input"})
			return
		}
		res, err := de.CreateRecord(*pp.Table, rb)
		if err != nil {
			c := http.StatusInternalServerError
			if ae, ok := err.(apiError); ok {
				c = ae.HTTPStatus
			}
			writeJSON(w, c, &responseEnvelope{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, &responseEnvelope{Response: res})
	default:
		writeJSON(w, http.StatusNotAcceptable, &responseEnvelope{Error: "bad method"})
	}
}

func (de *dbExplorer) HandleRecord(w http.ResponseWriter, r *http.Request, pp *pathParams) {
	switch r.Method {
	case http.MethodGet:
		res, err := de.GetRecordById(*pp.Table, *pp.ID)
		if err != nil {
			c := http.StatusInternalServerError
			if ae, ok := err.(apiError); ok {
				c = ae.HTTPStatus
			}
			writeJSON(w, c, &responseEnvelope{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, &responseEnvelope{Response: &recordResponse{Record: res}})
	default:
		writeJSON(w, http.StatusNotAcceptable, &responseEnvelope{Error: "bad method"})
	}
}
