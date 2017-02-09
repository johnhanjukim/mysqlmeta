package mysqlmeta

import (
	"database/sql"
	"encoding/json"
	"errors"
        "log"
	"reflect"
	"regexp"
	"strings"
	"unicode"
)

// treat as const
var SQL_INT_TYPE = regexp.MustCompile("(?i)^(tiny|small|medium||big)int(\\(\\d+\\))?$")
var SQL_UINT_TYPE = regexp.MustCompile("(?i)^(tiny|small|medium||big)int(\\(\\d+\\))? unsigned$")
var SQL_FLOAT_TYPE = regexp.MustCompile("(?i)^(float|double)(\\(\\d+\\))?( unsigned)?$")
var SQL_STRING_TYPE = regexp.MustCompile("(?i)^((char|varchar|binary|varbinary)(\\(\\d+\\))?|text|blob|enum.*)$")

type IndexMetadata struct {
	TableName    string  `json:"table_name"`
	NonUnique    bool    `json:"non_unique,omitempty"`
	KeyName      string  `json:"key_name"`
	SeqInIndex   uint    `json:"seq_in_index"`
	ColumnName   string  `json:"column_name"`
	Collation    *string `json:"collation"`
	Cardinality  uint    `json:"cardinality"`
	SubPart      *uint   `json:"sub_part,omitempty"`
	Packed       *string `json:"packed,omitempty"`
	Null         string  `json:"null,omitempty"`
	IndexType    string  `json:"index_type,omitempty"`
	Comment      string  `json:"comment,omitempty"`
	IndexComment string  `json:"index_comment,omitempty"`
}

type ColumnMetadata struct {
	Field        string          `json:"field,omitempty"`
	ColumnType   string          `json:"column_type,omitempty"`
	Nullable     string          `json:"nullable,omitempty"`
	Key          string          `json:"key,omitempty"`
	DefaultValue string          `json:"default_value,omitempty"`
	Extra        string          `json:"extra,omitempty"`
	StructField  string          `json:"struct_field,omitempty"`
	NoInsert     bool            `json:"no_insert,omitempty"`
	NoUpdate     bool            `json:"no_update,omitempty"`
	Indexes      []IndexMetadata `json:"indexes,omitempty"`
}

type TableMetadata struct {
	DB             *sql.DB          `json:"-"`
	Name           string           `json:"name,omitempty"`
	Columns        []ColumnMetadata `json:"columns,omitempty"`
	InsertColumns  []ColumnMetadata `json:"-"`
	UpdateColumns  []ColumnMetadata `json:"-"`
	ColumnNames    string           `json:"column_names,omitempty"`
	SelectString   string           `json:"select_string,omitempty"`
	InsertString   string           `json:"insert_string,omitempty"`
	UpdateString   string           `json:"update_string,omitempty"`
	EntityType     reflect.Type     `json:"-"`
	EntityTypeName string           `json:"type_name,omitempty"`
	FieldByColumn  map[string]int   `json:"field_by_name,omitempty"`
	Warn           string           `json:"warn,omitempty"`
}

func CamelCaseToSnakeCase(snakeCaseName string) string {
	// This matches MySQL snake-case (ex. "order_id") to Golang camelcase (ex. "OrderId").
	result := ""
	for i, c := range snakeCaseName {
		if (0 != i) && unicode.IsUpper(c) {
			result += "_"
		}
		result += string(unicode.ToLower(c))
	}
	return result
}

func SnakeCaseToCamelCase(snakeCaseName string) string {
	// This matches MySQL snake-case (ex. "order_id") to Golang camelcase (ex. "OrderId").
	wordStart := regexp.MustCompile(`(^\w|_\w)`) // matches first letter, or any letter after underscore
	replace := func(w string) string {
		// strings.Title capitalizes first letter, while TrimPrefix removes prefix string
		return strings.Title(strings.TrimPrefix(w, "_"))
	}
	return wordStart.ReplaceAllStringFunc(snakeCaseName, replace)
}

func (col ColumnMetadata) AllowInsert(val reflect.Value) bool {
	// Struct fields can use StructTag of sql:"no-insert" to disallow insert of that field
	// cf. https://golang.org/pkg/reflect/#example_StructTag
	return "id" != col.Field && !col.NoInsert
}

func (col ColumnMetadata) AllowUpdate(val reflect.Value) bool {
	// Struct fields can use StructTag of sql:"no-update" to disallow update of that field
	// cf. https://golang.org/pkg/reflect/#example_StructTag
	return "id" != col.Field && !col.NoUpdate
}

func GetValueId(value reflect.Value) uint {
	return uint(value.FieldByName("Id").Uint())
}

func SetValueId(value reflect.Value, id uint) {
	value.FieldByName("Id").SetUint(uint64(id))
}

func GetColumns(db *sql.DB, tableName string) ([]ColumnMetadata, error) {
	rows, err := db.Query("SHOW COLUMNS FROM `"+tableName+"`")
	if nil != err {
		log.Printf("sql query failed: %v", err)
		return nil, err
	}
	defer rows.Close()
	cols := []ColumnMetadata{}
	for rows.Next() {
		// SHOW COLUMNS returns field, type, nullable, key, default, extra
		col := ColumnMetadata{}
		rows.Scan(&col.Field, &col.ColumnType, &col.Nullable, &col.Key, &col.DefaultValue, &col.Extra)
		if nil != err {
			log.Printf("problem parsing column metadata for %v\n%v", tableName, err)
			return nil, err
		} else {
			cols = append(cols, col)
		}
	}
	return cols, nil
}

func GetIndexes(db *sql.DB, tableName string, cols []ColumnMetadata) ([]ColumnMetadata, error) {
	rows, err := db.Query("SHOW INDEXES FROM ?", tableName)
	if nil != err {
		log.Printf("sql query failed\n%v", err)
		return nil, err
	}
	defer rows.Close()
	// Create a map of column names to column indexes
	imap := map[string]int{}
	for i, _ := range cols {
		imap[cols[i].Field] = i
	}
	for rows.Next() {
		// SHOW INDEXES returns Table, Non_unique, Key_name, Seq_in_index, Column_name,
		// Collation, Cardinality, Sub_part, Packed, Null, Index_type, Comment, Index_comment
		ind := IndexMetadata{}
		err = rows.Scan(
			&ind.TableName,
			&ind.NonUnique,
			&ind.KeyName,
			&ind.SeqInIndex,
			&ind.ColumnName,
			&ind.Collation,
			&ind.Cardinality,
			&ind.SubPart,
			&ind.Packed,
			&ind.Null,
			&ind.IndexType,
			&ind.Comment,
			&ind.IndexComment,
		)
		if nil != err {
			log.Printf("problem parsing index metadata\n%v", err)
			return nil, err
		} else {
			// find the correct column to append this to
			i, ok := imap[ind.ColumnName]
			if ok {
				if nil == cols[i].Indexes {
					cols[i].Indexes = []IndexMetadata{}
				}
				cols[i].Indexes = append(cols[i].Indexes, ind)
			}
		}
	}
	return cols, nil
}

func CheckTableName(tableName string) error {
	validTableName := regexp.MustCompile("^[a-zA-Z_]+$")
	if validTableName.MatchString(tableName) {
		return nil
	} else {
		return errors.New("invalid table name")
	}
}

func GetStructValue(entity interface{}) (reflect.Value, error) {
	// The input to FetchTableMetadata and ScanEntity should be a pointer to a struct,
	// which we reflect on to dynamically fill in values.
	// This returns an error if the interface is not a pointer to a struct,
	// and the reflect.Value if successful.
	v := reflect.ValueOf(entity)
	if reflect.Ptr == v.Kind() {
		e := v.Elem()
		if e.IsValid() && (e.Kind() == reflect.Struct) {
			return e, nil
		}
	}
	log.Printf("invalid input to internal call - require pointer to struct\n%v", v.Kind())
	return reflect.ValueOf(nil), errors.New("invalid pointer argument")
}

// returns true if field matches db column, or false if there is a mismatch warning
func (col ColumnMetadata) CheckFieldType(tableName string, field reflect.StructField) bool {
	valid := true
	fieldType := field.Type
	if reflect.Ptr == fieldType.Kind() {
		fieldType = fieldType.Elem()
		if "YES" != col.Nullable {
			valid = false
		}
	} else {
		if "NO" != col.Nullable {
			valid = false
		}
	}
	if !valid {
		log.Printf("mismatch of nullable for column %s.%s", tableName, col.Field)
		return false
	}
	switch fieldType.Kind() {
	case reflect.Bool:
		valid = (col.ColumnType == "tinyint(1) unsigned")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		valid = SQL_INT_TYPE.MatchString(col.ColumnType)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		valid = SQL_UINT_TYPE.MatchString(col.ColumnType)
	case reflect.Float32, reflect.Float64:
		valid = SQL_FLOAT_TYPE.MatchString(col.ColumnType)
	case reflect.String, reflect.Struct:
		valid = SQL_STRING_TYPE.MatchString(col.ColumnType)
	}
	if !valid {
		log.Printf("mismatch of type for column")
		// "tableName":     tableName,
		// "sqlColumnName": col.Field,
		// "sqlColumnType": col.ColumnType,
		// "fieldName":     field.Name,
		// "fieldType":     fieldType.Kind(),
		return false
	}
	return true
}

func (metadata TableMetadata) CheckFieldTypes(entity interface{}) (string, error) {
	value, err := GetStructValue(entity)
	if nil != err {
		return "", err
	}
	entityType := value.Type()
	warn := ""
	sep := ""
	for _, col := range metadata.Columns {
		field := entityType.Field(metadata.FieldByColumn[col.Field])
		if !col.CheckFieldType(metadata.Name, field) {
			warn += (sep + col.Field)
			sep = ","
		}
	}
	if "" != warn {
		return "Warning: mismatched type in columns " + warn, nil
	} else {
		return "", nil
	}
}

func (col ColumnMetadata) GetMatchingFieldIndex(entityType reflect.Type) int {
	// Given an SQL column and a struct Type, this returns the index of the
	// corresponding field in the struct for that SQL column.
	match := -1
	camelCaseName := SnakeCaseToCamelCase(col.Field)
	for i := 0; i < entityType.NumField(); i++ {
		if camelCaseName == entityType.Field(i).Name {
			// This records the index of the matching struct field
			match = i
			break
		}
	}
	if -1 == match {
		log.Printf("failed to match column %s into entity type %v", col.Field, entityType.Name())
	}
	return match
}

func (col *ColumnMetadata) ReadSqlStructTags(field reflect.StructField) error {
	tagString := field.Tag.Get("sql")
	if "" != tagString {
		for i, tag := range strings.Split(tagString, ",") {
			switch tag {
			case "no-insert":
				col.NoInsert = true
			case "no-update":
				col.NoUpdate = true
			default:
				if 0 == i {
					col.StructField = tag
				} else {
					log.Printf(
						"unrecognized tag in sql StructTag for col %v\n%v\n%v",
						col.Field,
						tagString,
						tag,
					)
				}
			}
		}
	}
	return nil
}

func (metadata *TableMetadata) FetchTableMetadata(db *sql.DB, tableName string, entity interface{}) error {
	// check if metadata is already filled in - if so, do nothing
	if (nil != metadata) && ("" != metadata.Name) {
		return nil
	}
	// check that there is a valid tableName
	err := CheckTableName(tableName)
	if nil != err {
		return err
	}
	// check that this is a proper pointer to a struct
	value, err := GetStructValue(entity)
	if nil != err {
		return err
	}
	// store the database for future use
	metadata.DB = db
	// access the database and get the column definitions for this table
	cols, err := GetColumns(db, tableName)
	if nil != err {
		return err
	}
	// append index information into the column metadata
	cols, err = GetIndexes(db, tableName, cols)
	if nil != err {
		return err
	}
	// get the column names as a comma-separated list for use in SQL statements
	selectColNames := ""
	separator := ""
	for _, col := range cols {
		selectColNames += (separator + "`" + col.Field + "`")
		separator = ", "
	}
	selectString := "SELECT " + selectColNames + " FROM `" + tableName + "` "

	// Use reflect to create a map of SQL names to field indexes of the given type
	entityType := value.Type()

	// Map the MySQL columns to the struct fields
	fieldByColumn := map[string]int{}
	allMatched := true
	for i, col := range cols {
		fieldByColumn[col.Field] = cols[i].GetMatchingFieldIndex(entityType)
		if 0 > fieldByColumn[col.Field] {
			// a negative index indicates that no matching field was found
			allMatched = false
		} else {
			cols[i].ReadSqlStructTags(entityType.Field(fieldByColumn[col.Field]))
		}
	}
	if !allMatched {
		log.Printf("not all columns found match in table %v entity struct %v", tableName, entityType.Name())
		return errors.New("not all columns matched entity struct")
	}

	// get column names for INSERT (not including id or explicitly excluded fields)
	insertCols := []ColumnMetadata{}
	insertColNames := ""
	placeholders := ""
	separator = ""
	for _, col := range cols {
		if col.AllowInsert(value.Field(fieldByColumn[col.Field])) {
			insertCols = append(insertCols, col)
			insertColNames += (separator + "`" + col.Field + "`")
			placeholders += (separator + "?")
			separator = ", "
		}
	}
	insertString := "INSERT INTO `" + tableName + "` (" + insertColNames + ") VALUES (" + placeholders + ") "

	// get column names for UPDATE
	updateCols := []ColumnMetadata{}
	updateColNames := ""
	separator = ""
	for _, col := range cols {
		if col.AllowUpdate(value.Field(fieldByColumn[col.Field])) {
			updateCols = append(updateCols, col)
			updateColNames += (separator + "`" + col.Field + "`=?")
			separator = ", "
		}
	}
	updateString := "UPDATE `" + tableName + "` SET " + updateColNames + " "
	*metadata = TableMetadata{
		Name:           tableName,
		Columns:        cols,
		InsertColumns:  insertCols,
		UpdateColumns:  updateCols,
		ColumnNames:    selectColNames,
		SelectString:   selectString,
		InsertString:   insertString,
		UpdateString:   updateString,
		EntityType:     entityType,
		EntityTypeName: entityType.Name(),
		FieldByColumn:  fieldByColumn,
	}
	// fill in warnings for column types
	metadata.Warn, err = metadata.CheckFieldTypes(entity)
	return err
}

func (metadata TableMetadata) IsColumn(colname string) bool {
	_, ok := metadata.FieldByColumn[colname]
	return ok
}

func (metadata TableMetadata) ScanEntity(entity interface{}, rows *sql.Rows) error {
	// check that this is a proper pointer to a struct
	value, err := GetStructValue(entity)
	if nil != err {
		return err
	}
	values := make([]interface{}, len(metadata.Columns))
	jsonValues := make([]string, len(metadata.Columns))
	isJson := make([]bool, len(metadata.Columns))

	for i, col := range metadata.Columns {
		j := metadata.FieldByColumn[col.Field]
		if j < 0 {
			msg := "no matching field for column "+col.Field
			return errors.New(msg)
		}
		// If the field is string to be read into a struct, then
		// scan the SQL output as a JSON string.
		// This will then be converted after Scan is complete.
		if value.Field(j).Kind() == reflect.Struct {
			isJson[i] = true
			values[i] = &jsonValues[i]
		} else {
			values[i] = value.Field(j).Addr().Interface()
		}
	}
	err = rows.Scan(values...)
	if nil != err {
		log.Printf("failed to scan entity\n%v", err)
		return err
	}
	// For marked JSON field, convert JSON into the struct
	for i, col := range metadata.Columns {
		if isJson[i] {
			j := metadata.FieldByColumn[col.Field]
			err = json.Unmarshal([]byte(jsonValues[i]), value.Field(j).Addr().Interface())
			if nil != err {
				return errors.New("cannot unmarshal json field")
			}
		}
	}
	return nil
}

func (metadata TableMetadata) GetRows(clause string, v ...interface{}) (*sql.Rows, error) {
	query := metadata.SelectString + clause
	rows, err := metadata.DB.Query(query, v...)
	if nil != err {
		log.Printf("error making given query\n%v\n%v", query, err)
		if nil != rows {
			rows.Close()
		}
		return nil, err
	}
	return rows, nil
}

func (metadata TableMetadata) GetEntity(entity interface{}, clause string, v ...interface{}) (interface{}, error) {
	// Note that this returns the first matching database row.
	// It does not detect multiple results.
	query := metadata.SelectString + clause
	rows, err := metadata.DB.Query(query, v...)
	defer rows.Close()
	if nil != err {
		log.Printf("error making given query\n%v\n%v", query, err)
		return nil, err
	} else if rows.Next() {
		return entity, metadata.ScanEntity(entity, rows)
	} else {
		// No entity was found - return nil to indicate blank
		return nil, nil
	}
}

func (metadata TableMetadata) GetEntityById(entity interface{}, id uint) (interface{}, error) {
	return metadata.GetEntity(entity, " WHERE id = ?", id)
}

func (metadata TableMetadata) GetEntityByColumn(entity interface{}, colname string, v interface{}) (interface{}, error) {
	if !metadata.IsColumn(colname) {
		log.Printf("invalid column name for given table %v.%v", metadata.Name, colname)
		return nil, errors.New("invalid column name")
	}
	return metadata.GetEntity(entity, " WHERE `"+colname+"` = ?", v)
}

func (metadata TableMetadata) GetColumnValue(value reflect.Value, col ColumnMetadata) (interface{}, error) {
	j := metadata.FieldByColumn[col.Field]
	if value.Field(j).Type().Kind() == reflect.Struct {
		// Convert entity struct field into JSON for insert/update in database.
		// The value is converted into a byte array.
		jsonByteValue, err := json.Marshal(value.Field(j).Addr().Interface())
		if err != nil {
			return "{}", errors.New("unable to convert struct field to json")
		}
		return jsonByteValue, nil
	}
	return value.Field(j).Interface(), nil
}

// TODO: create a GetEntityByColumns that allows multiple column specifications
// GetEntityByColumns(entity interface{}, match map[string]interface{}) (interface{}, error) {

func (metadata TableMetadata) insertEntityValue(entity interface{}, value reflect.Value) (uint, error) {
	values := make([]interface{}, len(metadata.InsertColumns))
	for i, col := range metadata.InsertColumns {
		columnValue, err := metadata.GetColumnValue(value, col)
		if nil != err {
			return uint(0), err
		}
		values[i] = columnValue
	}
	result, err := metadata.DB.Exec(metadata.InsertString, values...)
	if nil != err {
		return 0, err
	}
	id, err := result.LastInsertId()
	if nil != err {
		return 0, err
	}
	SetValueId(value, uint(id))
	return uint(id), nil
}

func (metadata TableMetadata) updateEntityValue(entity interface{}, value reflect.Value) error {
	// This requires an entity id field
	id := GetValueId(value)
	if 0 == id {
		return errors.New("no defined id for update")
	}
	// Collect the values for the update query
	values := make([]interface{}, len(metadata.UpdateColumns)+1)
	for i, col := range metadata.UpdateColumns {
		columnValue, err := metadata.GetColumnValue(value, col)
		if nil != err {
			return err
		}
		values[i] = columnValue
	}
	values[len(metadata.UpdateColumns)] = id
	q := metadata.UpdateString+" WHERE id = ?"
	result, err := metadata.DB.Exec(q, values...)
	if nil != err {
		return err
	}
	rows, err := result.RowsAffected()
	if nil != err {
		return err
	}
	if 1 != rows {
		log.Printf("update modified more or less than one row %v\n%v", rows, q)
		return nil
	}
	return nil

}

func (metadata TableMetadata) InsertEntity(entity interface{}) (uint, error) {
	// check that this is a proper pointer to a struct
	value, err := GetStructValue(entity)
	if nil != err {
		return 0, err
	}
	return metadata.insertEntityValue(entity, value)
}

func (metadata TableMetadata) UpdateEntity(entity interface{}) error {
	// check that this is a proper pointer to a struct
	value, err := GetStructValue(entity)
	if nil != err {
		return err
	}
	return metadata.updateEntityValue(entity, value)
}

func (metadata TableMetadata) SaveEntity(entity interface{}) (uint, error) {
	// check that this is a proper pointer to a struct
	value, err := GetStructValue(entity)
	if nil != err {
		return 0, err
	}
	id := GetValueId(value)
	if 0 == id {
		return metadata.insertEntityValue(entity, value)
	} else {
		return id, metadata.updateEntityValue(entity, value)
	}
}

func (metadata TableMetadata) DeleteEntity(entity interface{}) error {

	// TODO: determine from indexes all unique keys, and delete based on that,
	// or delete based only on id field
	return errors.New("not implemented yet")
}
