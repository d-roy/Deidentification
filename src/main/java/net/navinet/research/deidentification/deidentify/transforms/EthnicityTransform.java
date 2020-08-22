package net.navinet.research.deidentification.deidentify.transforms;
import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;


/**
 * Created by EForeman on 7/20/2016.
 */

final class EthnicityTransform extends AbstractDataFrameTransform {
    private static final Logger logger = Logger.getLogger(EthnicityTransform.class.getName());
    private static final String[] ethnicityH = {
                "hispanic or latino",
                "andalusian",
                "asturian",
                "aastillian",
                "catalonian",
                "belearic islander",
                "gallego",
                "valencian",
                "canarian",
                "spanish basque",
                "mexican",
                "mexican-american",
                "mexicano",
                "chicano",
                "la raza",
                "mexican-american indian",
                "central american",
                "costa rican",
                "guatemalan",
                "honduran",
                "nicaraguan",
                "panamanian",
                "salvadoran",
                "central american indian",
                "canal zone",
                "south american",
                "argentinean",
                "bolivian",
                "chilean",
                "colombian",
                "ecuadorian",
                "paraguayan",
                "peruvian",
                "uruguayan",
                "venezuelan",
                "south american indian",
                "criollo",
                "latin american",
                "puerto rican",
                "cuban",
                "dominican"
        };
        private static final List<String> ethnicityHispanic = Arrays.asList(ethnicityH);

        private static final String [] ethnicityA ={
                "afghan",
                "armenian",
                "azerbaijani",
                "bahraini",
                "bangladeshi",
                "bhutanese",
                "bruneian",
                "cambodian",
                "chinese",
                "cypriot",
                "east timorese",
                "egyptian",
                "georgian",
                "indian",
                "indonesian",
                "iranian",
                "persian",
                "iraqi",
                "israeli",
                "japanese",
                "jordanian",
                "kazakhstani",
                "kuwaiti",
                "kyrgyz",
                "lao",
                "laotian",
                "lebanese",
                "malaysian",
                "maldivian",
                "mongolian",
                "myanma",
                "burmese",
                "nepali",
                "nepalese",
                "gurkhas",
                "north korean",
                "omani",
                "pakistani",
                "filipino",
                "filipina",
                "philippine",
                "qatari",
                "russian",
                "saudi arabian",
                "singaporean",
                "south korean",
                "korean",
                "sri lankan",
                "syrian arabs",
                "arameans",
                "kurds",
                "turkomans",
                "tajikistani",
                "thai",
                "turkish",
                "turkmen",
                "emirati",
                "emiri",
                "uzbekistani",
                "vietnamese",
                "yemeni",
                "asian"
        };
        private static final List<String> ethnicityAsian = Arrays.asList(ethnicityA);

        private static final String[] ethnicityB = {
                "african american",
                "black",
                "black american",
                "west african",
                "central african",
                "south african",
                "east african",
                "caribbean",
        };

        private static final List<String> ethnicityBlack = Arrays.asList(ethnicityB);

        private static final String[] ethnicityN = {
            "pitcairn islander",
            "māori",
            "tokelauan",
            "tuvaluan",
            "native hawaiian",
            "polynesian",
            "tahitians",
            "togan",
            "samoan",
    };
        private static final List<String> ethnicityNativeHawaiian = Arrays.asList(ethnicityN);

        private static final String[] ethnicityAN ={
            "american indian",
            "tsimshian",
            "aleut",
            "eskimo",
            "hadia",
            "ilingit",
            "eyak",
            "alaskan athabaskans",

        };
        private static final List<String> ethnicityAlsakanNative = Arrays.asList(ethnicityAN);

        private static final String[] ethnicityW = {
                "white",
                "white american",
                "european american"
        };

        private static final List<String> ethnicityWhite= Arrays.asList(ethnicityW);


    EthnicityTransform(String columnName) { super(columnName);}


        /**
         * A peculiarity of the Java API for Spark is that user-defined functions must
         * be registered against the Spark context against which it is to be run. The
         * reason, for this, is that it is necessary to work with Java's type system.
         * <p>
         * In any case, applying a user-defined function to a column is a two-step
         * procedure, as per
         * <code>
         * sqlContext.udf().register("fn", fn, DataType);
         * dataFrame.withColumn(columnName, callUDF("fn", dataFrame.col(columnName));
         * </code>
         * <p>
         * This mapping creates a random ethnicity for every element in the column.
         *
         * @param sqlContext {@link SQLContext} to register this mapping to
         */
        static void register(SQLContext sqlContext) {
            sqlContext.udf().register("Ethnicity", new UDF1<String, String>() {
                @Override
                public String call(String string) throws Exception {
                    String transformedEthnicity = "";
                   try {
                           if (ethnicityHispanic.contains(string.toLowerCase())) {
                               transformedEthnicity = "Hispanic or Latino";
                           }
                           else if (ethnicityAsian.contains(string.toLowerCase())) {
                               transformedEthnicity = "Asian";
                           }
                           else if (ethnicityNativeHawaiian.contains(string.toLowerCase())) {
                               transformedEthnicity = "Native Hawaiian or" +
                                       "Other Pacific Islander";
                           }
                           else if (ethnicityBlack.contains(string.toLowerCase())) {
                               transformedEthnicity = "Black or African American";
                           }
                           else if (ethnicityAlsakanNative.contains(string.toLowerCase())) {
                           transformedEthnicity = "American Indian or Alaska Native";
                       }
                           else {
                               transformedEthnicity = "White";
                           }

                   }
                   catch (Exception error){
                       logger.error("This is not in the list of ethnicity's");
                   }
                   return transformedEthnicity;
                }
            }, DataTypes.StringType);
        }

    @Override
    public DataFrame transform(DataFrame input) {
        return input.withColumn(columnName, callUDF("Ethnicity", input.col(columnName)));
    }
}
