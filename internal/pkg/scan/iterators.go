package scan

import "github.com/unhandled-exception/sophiadb/internal/pkg/records"

// ForEach смещает указатель текущей запси в образе сканирования от первой до полследней
// и вызывает функцию call для каждой записи в образе сканирования с интерфйсом Scan.
// Проход по Scan останавливается принудительно, когда call вернула ошибку или stop == true.
// ForEach возвращает ошибку из call или nil.
func ForEach(ts Scan, call func() (stop bool, err error)) error {
	if err := ts.BeforeFirst(); err != nil {
		return err
	}

	for {
		ok, err := ts.Next()
		if !ok {
			if err != nil {
				return err
			}

			break
		}

		stop, err := call()
		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

// ForEachField вызывает метод call для каждого поля из схемы образа сканирования Scan.
// call получает в параметрах имя и тип поля.
// Проход по полям останавливается принудительно, когда call вернула ошибку или stop == true.
// ForEachField возвращает ошибку из call или nil.
func ForEachField(ts Scan, call func(name string, fieldType records.FieldType) (stop bool, err error)) error {
	for _, name := range ts.Schema().Fields() {
		fieldType := ts.Schema().Type(name)

		stop, err := call(name, fieldType)
		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

// ForEachValue вызывает метод call для каждого значения текущей записи образа сканирования Scan.
// call получает в параметрах имя поля, тип поля и нетипизированное значение поля.
// Проход по полям останавливается принудительно, когда call вернула ошибку или stop == true.
// ForEachValue возвращает ошибку из call или nil.
func ForEachValue(ts Scan, call func(name string, fieldType records.FieldType, value any) (stop bool, err error)) error {
	for _, name := range ts.Schema().Fields() {
		fieldType := ts.Schema().Type(name)

		var (
			value any
			err   error
		)

		//nolint:exhaustive
		switch fieldType {
		case records.Int64Field:
			value, err = ts.GetInt64(name)
		case records.Int8Field:
			value, err = ts.GetInt8(name)
		case records.StringField:
			value, err = ts.GetString(name)
		default:
			err = ErrUnknownFieldType
		}

		if err != nil {
			return err
		}

		stop, err := call(name, fieldType, value)
		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}
