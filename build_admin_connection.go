package kafka_dao

func (d *daoImpl) buildAdminConnection() {
	if d.admin.Get() == nil {
		d.admin.SetF(d.config.adminBuilder)
	}
}
