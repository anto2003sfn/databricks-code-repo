databricks bundle summary
if [ $? -eq 0 ]; then
	databricks bundle validate
	if [ $? -eq 0 ] ; then
		databricks bundle deploy -t dev
	        if [ $? -eq 0 ]; then
	          	databricks bundle run retail_etl_scheduling_pipeline -t dev
		fi
	fi
fi

