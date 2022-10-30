package metadata

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

const (
	MaxViewNameLength = MaxTableNameLength
	MaxViewsDefLength = 200

	viewsCatalogTableName = "sdb_views"

	VcatViewNameField = "viewname"
	VcatViewDefField  = "viewdef"
)

type Views struct {
	VcatTableName string
	VcatLayout    records.Layout

	tables TablesManager
}

func NewViews(tables TablesManager, isNew bool, trx scan.TRXInt) (*Views, error) {
	v := &Views{
		VcatTableName: viewsCatalogTableName,
		VcatLayout:    newViewsCatalogLayout(),

		tables: tables,
	}

	if isNew {
		if err := tables.CreateTable(v.VcatTableName, v.VcatLayout.Schema, trx); err != nil {
			return nil, errors.WithMessage(ErrViewsMetadata, err.Error())
		}
	}

	return v, nil
}

func newViewsCatalogLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddStringField(VcatViewNameField, MaxViewNameLength)
	schema.AddStringField(VcatViewDefField, MaxViewsDefLength)

	return records.NewLayout(schema)
}

func (v *Views) ViewExists(viewName string, trx scan.TRXInt) (bool, error) {
	found := false

	vcat, err := v.newViewCatalogTableScan(trx)
	if err != nil {
		return false, v.wrapError(err, viewName, nil)
	}

	defer vcat.Close()

	if err := scan.ForEach(vcat, func() (bool, error) {
		name, err := vcat.GetString(VcatViewNameField)
		if err != nil {
			return true, err
		}

		found = (name == viewName)

		return found, nil
	}); err != nil {
		return false, v.wrapError(err, viewName, nil)
	}

	return found, nil
}

func (v *Views) CreateView(viewName string, viewDef string, trx scan.TRXInt) error {
	exists, err := v.ViewExists(viewName, trx)
	if err != nil {
		return v.wrapError(err, viewName, nil)
	}

	if exists {
		return ErrViewExists
	}

	vcat, err := v.newViewCatalogTableScan(trx)
	if err != nil {
		return v.wrapError(err, viewName, nil)
	}

	defer vcat.Close()

	if err := vcat.Insert(); err != nil {
		return v.wrapError(err, viewName, nil)
	}

	if err := scan.ForEachField(vcat, func(name string, fieldType records.FieldType) (bool, error) {
		var err error

		switch name {
		case VcatViewNameField:
			err = vcat.SetString(VcatViewNameField, viewName)
		case VcatViewDefField:
			err = vcat.SetString(VcatViewDefField, viewDef)
		}

		return false, err
	}); err != nil {
		return v.wrapError(err, viewName, nil)
	}

	return nil
}

func (v *Views) ViewDef(viewName string, trx scan.TRXInt) (string, error) {
	var viewDef string

	found := false

	vcat, err := v.newViewCatalogTableScan(trx)
	if err != nil {
		return "", v.wrapError(err, viewName, nil)
	}

	defer vcat.Close()

	if err := scan.ForEach(vcat, func() (bool, error) {
		name, err := vcat.GetString(VcatViewNameField)
		if err != nil {
			return true, err
		}

		if name == viewName {
			viewDef, err = vcat.GetString(VcatViewDefField)
			if err != nil {
				return true, err
			}

			found = true

			return true, nil
		}

		return false, nil
	}); err != nil {
		return "", v.wrapError(err, viewName, nil)
	}

	if !found {
		return "", ErrViewNotFound
	}

	return viewDef, nil
}

func (v *Views) newViewCatalogTableScan(trx scan.TRXInt) (*scan.TableScan, error) {
	vcat, err := scan.NewTableScan(trx, v.VcatTableName, v.VcatLayout)
	if err != nil {
		return nil, err
	}

	return vcat, nil
}

func (v *Views) wrapError(err error, viewName string, baseError error) error {
	if baseError == nil {
		baseError = ErrTablesMetadata
	}

	return errors.WithMessagef(baseError, "view %s: %s", viewName, err)
}
