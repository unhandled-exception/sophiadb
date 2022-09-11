package scan

import "github.com/unhandled-exception/sophiadb/pkg/records"

func ForEach(ts Scan, call func() (bool, error)) error {
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

func ForEachField(ts Scan, call func(name string, fieldType records.FieldType) (bool, error)) error {
	for _, name := range ts.Layout().Schema.Fields() {
		fieldType := ts.Layout().Schema.Type(name)

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

func ForEachValue(ts Scan, call func(name string, fieldType records.FieldType, value interface{}) (bool, error)) error {
	for _, name := range ts.Layout().Schema.Fields() {
		fieldType := ts.Layout().Schema.Type(name)

		var (
			value interface{}
			err   error
		)

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
