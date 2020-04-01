package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

type fieldMetaInfo struct {
	fieldName  string
	fieldType  string
	collation  sql.NullString
	isNull     string
	key        string
	isDefault  sql.NullString
	extra      string
	privileges string
	comment    string
}

type jsonNullString struct {
	sql.NullString
}

func (v jsonNullString) MarshalJSON() ([]byte, error) {
	if v.Valid {
		return json.Marshal(v.String)
	} else {
		return json.Marshal(nil)
	}
}

func (v *jsonNullString) UnmarshalJSON(data []byte) error {
	// Unmarshalling into a pointer will let us detect null
	var x *string
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	if x != nil {
		v.Valid = true
		v.String = *x
	} else {
		v.Valid = false
	}
	return nil
}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
type dBExplorer struct {
	db                 *sql.DB
	tablesWithMetaInfo map[string][]*fieldMetaInfo
	tableWithIdPath    *regexp.Regexp
	tablePath          *regexp.Regexp
}

func (d *dBExplorer) getAllTables(w http.ResponseWriter, r *http.Request) {
	resp := make(map[string][]string)
	w.WriteHeader(http.StatusOK)

	tableNames := make([]string, 0, len(d.tablesWithMetaInfo))
	for key := range d.tablesWithMetaInfo {
		tableNames = append(tableNames, key)
	}

	resp["tables"] = tableNames

	data, err := json.Marshal(resp)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}

func (d *dBExplorer) getTableRow(w http.ResponseWriter, r *http.Request) {
	urlParams := d.tableWithIdPath.FindStringSubmatch(r.URL.Path)
	log.Println(urlParams)
	tableName := urlParams[1]
	rowId := urlParams[2]

	selectQuery := fmt.Sprintf(`SELECT * FROM %s WHERE `, tableName)

	tableFields, ok := d.tablesWithMetaInfo[tableName]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	idFieldName := ""

	for _, val := range tableFields {
		if val.key == "PRI" {
			idFieldName = val.fieldName
			break
		}
	}

	selectQuery += idFieldName + " = ?"

	log.Println(selectQuery)
	resultResp := make(map[string]interface{})

	row, err := d.db.Query(selectQuery, rowId)
	defer row.Close()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	columns, err := row.ColumnTypes()

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fields := make([]interface{}, len(columns))

	for row.Next() {
		for i, val := range columns {
			var v interface{}
			log.Println(val.DatabaseTypeName())

			switch val.DatabaseTypeName() {
			case "TEXT", "VARCHAR":
				nullable, _ := val.Nullable()
				if nullable {
					log.Println("Value is nullable")
					v = &jsonNullString{}
				} else {
					v = new(string)
				}
			default:
				v = new(interface{})
			}
			resultResp[val.Name()] = v
			fields[i] = resultResp[val.Name()]
		}

		err = row.Scan(fields...)

		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	log.Println(resultResp)
	w.WriteHeader(http.StatusOK)
	data, err := json.MarshalIndent(resultResp, "", "\t")

	w.Write(data)
}

func (d *dBExplorer) getTableRecords(w http.ResponseWriter, r *http.Request) {
	urlParams := d.tablePath.FindStringSubmatch(r.URL.Path)
	log.Println(urlParams)
	tableName := urlParams[1]
	limit := r.FormValue("limit")
	if limit == "" {
		limit = "5"
	}
	offset := r.FormValue("offset")
	if offset == "" {
		offset = "0"
	}
	intLimitValue, err := strconv.Atoi(limit)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	intOffsetValue, err := strconv.Atoi(offset)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	selectQuery := fmt.Sprintf(`SELECT * FROM %s LIMIT ? OFFSET ?`, tableName)

	log.Println(selectQuery)

	row, err := d.db.Query(selectQuery, intLimitValue, intOffsetValue)
	defer row.Close()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	columns, err := row.ColumnTypes()

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resultResp := make([]map[string]interface{}, 0, intLimitValue)

	fields := make([]interface{}, len(columns))

	for row.Next() {
		resRow := make(map[string]interface{})
		for i, val := range columns {
			var v interface{}
			log.Println(val.DatabaseTypeName())

			switch val.DatabaseTypeName() {
			case "TEXT", "VARCHAR":
				nullable, _ := val.Nullable()
				if nullable {
					log.Println("Value is nullable")
					v = &jsonNullString{}
				} else {
					v = new(string)
				}
			default:
				v = new(interface{})
			}
			resRow[val.Name()] = v
			fields[i] = resRow[val.Name()]
		}

		err = row.Scan(fields...)

		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		resultResp = append(resultResp, resRow)
	}

	log.Println(resultResp)
	w.WriteHeader(http.StatusOK)
	data, err := json.MarshalIndent(resultResp, "", "\t")

	w.Write(data)
}

func (d *dBExplorer) putTableRecord(w http.ResponseWriter, r *http.Request) {
	urlParams := d.tablePath.FindStringSubmatch(r.URL.Path)
	log.Println(urlParams)
	tableName := urlParams[1]

	req := make(map[string]interface{})

	err := json.NewDecoder(r.Body).Decode(&req)

	log.Println("Request is ", req)

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	tableFields, ok := d.tablesWithMetaInfo[tableName]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var reqFieldNames []string
	for key := range req {
		reqFieldNames = append(reqFieldNames, key)
	}

	log.Println("reqFieldNames: ", reqFieldNames)

	//create field names set
	fieldNamesSet := make(map[string]struct{})
	for _, tableVal := range tableFields {
		fieldNamesSet[tableVal.fieldName] = struct{}{}
	}

	log.Println("fieldNamesSet: ", fieldNamesSet)

	// validate request fields / remove fields from request that not match
	for _, val := range reqFieldNames {
		if _, ok := fieldNamesSet[val]; !ok {
			log.Println("Val is ", val)
			delete(req, val)
		}
	}

	log.Println("Request is ", req)

	// remove id for autoincrement fields
	for _, tableVal := range tableFields {
		if _, ok := req[tableVal.fieldName]; ok && tableVal.key == "PRI" {
			delete(req, tableVal.fieldName)
		}
	}

	log.Println("Request after validation is ", req)

	// insert non existing values
	for _, tableVal := range tableFields {
		if _, ok := req[tableVal.fieldName]; !ok {
			if (tableVal.isNull == "NO") && !tableVal.isDefault.Valid && tableVal.key != "PRI" {
				switch {
				case tableVal.fieldType == "int":
					req[tableVal.fieldName] = 0
				case tableVal.fieldType == "text", strings.Contains(tableVal.fieldType, "varchar"):
					req[tableVal.fieldName] = ""
				}
			}
		}
	}

	log.Println("After default fill", req)

	insertQuery := fmt.Sprintf(`INSERT INTO %s (`, tableName)

	placeholders := make([]string, 0, len(req))
	reqFieldNames = make([]string, 0, len(req))
	for key := range req {
		reqFieldNames = append(reqFieldNames, key)
		placeholders = append(placeholders, "?")
	}

	reqFieldNamesString := strings.Join(reqFieldNames, ",")
	placeholdersString := strings.Join(placeholders, ",")
	insertQuery += reqFieldNamesString + ") VALUES (" + placeholdersString + ")"

	var values []interface{}

	for _, val := range reqFieldNames {
		values = append(values, req[val])
	}

	log.Println("insertQuery is ", insertQuery)

	log.Println("values is ", values)

	_, err = d.db.Exec(insertQuery, values...)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (d *dBExplorer) updateTableRow(w http.ResponseWriter, r *http.Request) {
	urlParams := d.tableWithIdPath.FindStringSubmatch(r.URL.Path)
	log.Println(urlParams)
	tableName := urlParams[1]
	rowId := urlParams[2]

	req := make(map[string]interface{})

	err := json.NewDecoder(r.Body).Decode(&req)

	log.Println("Request is ", req)

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	tableFields, ok := d.tablesWithMetaInfo[tableName]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var reqFieldNames []string
	for key := range req {
		reqFieldNames = append(reqFieldNames, key)
	}

	log.Println("reqFieldNames: ", reqFieldNames)

	//create field names set
	fieldNamesSet := make(map[string]struct{})
	for _, tableVal := range tableFields {
		fieldNamesSet[tableVal.fieldName] = struct{}{}
	}

	log.Println("fieldNamesSet: ", fieldNamesSet)

	// validate request fields / remove fields from request that not match
	for _, val := range reqFieldNames {
		if _, ok := fieldNamesSet[val]; !ok {
			log.Println("Val is ", val)
			delete(req, val)
		}
	}

	log.Println("Request is ", req)

	idFieldName := ""
	// remove id for autoincrement fields
	for _, tableVal := range tableFields {
		if tableVal.key == "PRI" {
			idFieldName = tableVal.fieldName
		}

		if _, ok := req[tableVal.fieldName]; ok && tableVal.key == "PRI" {
			delete(req, tableVal.fieldName)
		}
	}

	log.Println("Request after validation is ", req)

	// insert non existing values
	for _, tableVal := range tableFields {
		if _, ok := req[tableVal.fieldName]; !ok {
			if (tableVal.isNull == "NO") && !tableVal.isDefault.Valid && tableVal.key != "PRI" {
				switch {
				case tableVal.fieldType == "int":
					req[tableVal.fieldName] = 0
				case tableVal.fieldType == "text", strings.Contains(tableVal.fieldType, "varchar"):
					req[tableVal.fieldName] = ""
				}
			}
		}
	}

	log.Println("After default fill", req)

	updateQuery := fmt.Sprintf(`UPDATE %s SET `, tableName)

	reqFieldNames = make([]string, 0, len(req))
	for key := range req {
		reqFieldNames = append(reqFieldNames, key+" = ?")
	}

	reqFieldNamesString := strings.Join(reqFieldNames, ",")
	updateQuery += reqFieldNamesString

	updateQuery += fmt.Sprintf(" WHERE %s = ?", idFieldName)

	var values []interface{}
	for _, val := range reqFieldNames {
		values = append(values, req[strings.TrimSuffix(val, " = ?")])
	}
	values = append(values, rowId)

	log.Println("updateQuery is ", updateQuery)

	log.Println("values is ", values)

	_, err = d.db.Exec(updateQuery, values...)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (d *dBExplorer) deleteTableRow(w http.ResponseWriter, r *http.Request) {
	urlParams := d.tableWithIdPath.FindStringSubmatch(r.URL.Path)
	log.Println(urlParams)
	tableName := urlParams[1]
	rowId := urlParams[2]

	deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE `, tableName)

	tableFields, ok := d.tablesWithMetaInfo[tableName]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	idFieldName := ""

	for _, val := range tableFields {
		if val.key == "PRI" {
			idFieldName = val.fieldName
			break
		}
	}

	deleteQuery += idFieldName + " = ?"

	result, err := d.db.Exec(deleteQuery, rowId)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	affected, err := result.RowsAffected()

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	resp := `{"deleted": ` + strconv.Itoa(int(affected)) + `}`
	w.Write([]byte(resp))
}

func (d *dBExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Path
	log.Println("URL path is ", url)
	switch {
	case url == "/":
		log.Println("Here / ")
		switch r.Method {
		case "GET":
			d.getAllTables(w, r)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	case d.tableWithIdPath.MatchString(url):
		log.Println("Here /$table/$id ")
		switch r.Method {
		case "GET":
			d.getTableRow(w, r)
		case "POST":
			d.updateTableRow(w, r)
		case "DELETE":
			d.deleteTableRow(w, r)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	case d.tablePath.MatchString(url):
		log.Println("Here /$table ")

		switch r.Method {
		case "GET":
			d.getTableRecords(w, r)
		case "PUT":
			d.putTableRecord(w, r)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	res := &dBExplorer{db: db, tablesWithMetaInfo: make(map[string][]*fieldMetaInfo)}
	res.tableWithIdPath = regexp.MustCompile(`/([A-Za-z0-9]+)/([0-9]+)`)
	res.tablePath = regexp.MustCompile(`/([A-Za-z0-9]+)`)

	log.Println("INIT DB tables")
	tableNames, err := db.Query("SHOW TABLES")
	defer tableNames.Close()

	if err != nil {
		return nil, err
	}

	tableName := ""
	for tableNames.Next() {
		err = tableNames.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		log.Println(tableName)

		tableFields, err := db.Query(fmt.Sprintf(`SHOW FULL COLUMNS FROM %s`, tableName))

		if err != nil {
			return nil, err
		}

		for tableFields.Next() {
			tableField := &fieldMetaInfo{}

			err = tableFields.Scan(
				&tableField.fieldName,
				&tableField.fieldType,
				&tableField.collation,
				&tableField.isNull,
				&tableField.key,
				&tableField.isDefault,
				&tableField.extra,
				&tableField.privileges,
				&tableField.comment)

			if err != nil {
				tableFields.Close()
				return nil, err
			}
			log.Println(tableField)
			res.tablesWithMetaInfo[tableName] = append(res.tablesWithMetaInfo[tableName], tableField)
		}
		tableFields.Close()
	}

	log.Println(res.tablesWithMetaInfo)

	return res, nil
}
