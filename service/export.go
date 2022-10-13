package service

import "wea-recover/common/def"

func Export(param def.InputInfo) error {
	err := checkExportParam(param)
	if err != nil {
		return err
	}

	// export
	return export(param)
}

func checkExportParam(param def.InputInfo) error {
	return nil
}

func export(param def.InputInfo) error {
	return nil
}
