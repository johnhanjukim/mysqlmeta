# mysqlmeta

A go package for using MySQL metadata to map table data to structs using reflection. 

## Usage

The basic usage is quite easy. 

```
var meta mysqlmeta.TableMetadata
type Product struct {
        Id          uint
        Name        string `sql:"no-update"`
        Description string
}
func GetProductById(id uint) Product {
     product := Product{}
     err := FetchTableMetadata(&meta, "product", &product)
     if err != nil {
         return err
     }
     _, err := meta.GetEntityById(&product, id)
     return product, err
}

```

## Options

The struct can have "sql" tags to specify behavior. 

1) <name>: Optionally look for an sql name different than the struct field.
2) "no-update": This field is never updated once set. 
3) "no-insert": This field is not set upon insert.

```
type Product struct {
        Id          uint
        Name        string `sql:"no-insert,no-update"`
        Description string `sql:"descr,no-update"`
}
```

## Testing / Development
To run the tests you may need to adjust the configuration for a local database.
This uses identical option-setting to the mysql driver.
See the [Testing Wiki-Page](https://github.com/go-sql-driver/mysql/wiki/Testing "Testing") for details.

The  is not feature-complete yet. Help is very appreciated.

