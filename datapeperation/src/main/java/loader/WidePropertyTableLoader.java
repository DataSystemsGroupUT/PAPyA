package loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import statistics.DatabaseStatistics;

import java.util.Map;

/**
 * Loads a WPT named wide_property_table.
 * Schema: s|&lt p_0 &gt |... |&lt p_n &gt
 */
public class WidePropertyTableLoader extends PropertyTableLoader {

	private static final String PROPERTIES_CARDINALITIES_TABLE_NAME = "properties";
	private static final String WPT_TABLE_NAME = "wide_property_table";

	public WidePropertyTableLoader(final Settings settings,
								   final SparkSession spark, final DatabaseStatistics statistics) {
		super(settings.getDatabaseName(), spark, settings.isWptPartitionedBySubject(), WPT_TABLE_NAME, statistics);
	}

	@Override
	Dataset<Row> loadDataset() {
		//Create the Properities (0,1) Dataframe
		final Dataset<Row> propertiesCardinalities = calculatePropertiesComplexity(COLUMN_NAME_SUBJECT);
		//Save this DF of predicates in HDFS
		saveTable(propertiesCardinalities, PROPERTIES_CARDINALITIES_TABLE_NAME);
		//Create a MAP of the predicate and boolean (true if multi-valued).
		final Map<String, Boolean> propertiesCardinalitiesMap =
				createPropertiesComplexitiesMap(propertiesCardinalities);

		setPropertiesNames(propertiesCardinalitiesMap.keySet().toArray(new String[0]));

		return createPropertyTableDataset(propertiesCardinalitiesMap, COLUMN_NAME_SUBJECT,
				COLUMN_NAME_OBJECT);
	}
}
