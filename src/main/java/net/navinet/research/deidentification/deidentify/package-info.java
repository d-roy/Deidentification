/**
 * Package encapsulating transformations of {@link org.apache.spark.sql.DataFrame}s. The principle
 * class within this package is
 * {@link net.navinet.research.deidentification.deidentify.transforms.DataFrameTransformFactory},
 * which is used to create the transformations to be applied to DataFrames.
 *
 * The following types of transforms can be created using
 * {@link net.navinet.research.deidentification.deidentify.transforms.DataFrameTransformFactory#createTransform(net.navinet.research.deidentification.deidentify.ColumnStrategy)}
 *
 * <p>
 * <ul>
 * <li>{@code "Identity"} - For Strings. Return the same column.</li>
 * <li>{@code "Hash"} - For Strings. Puts the column through a Hashing algorithm.</li>
 * <li>{@code "Drop"} - For all types. Removes the column from the DataFrame.</li>
 * <li>{@code "Duplicate"} - For all types. Creates a new, identical column.</li>
 * <li>{@code "Zip3Digit"} - For Strings. Scrubs away zip information.</li>
 * <li>{@code "Year"} - For dates. Retains only year and days since altered birth date.</li>
 * <li>{@code "Age"} - for numbers. Returns the min(given, 90). </li>
 * <li>{@code "MaxYear"} - for numbers. Returns max(given, current year - 90)</li>
 * <li>{@code "RandomFirstName"} - for any. Returns a random first name</li>
 * <li>{@code "RandomLastName"} - for any. Returns a random last name</li>
 * <li>{@code "FixedFirstName"} - for any. Returns the string "FirstName"</li>
 * <li>{@code "FixedLastName"} - for any. Returns the string "LastName"</li>
 * </ul>
 * <p>
 *
 * This list is copied from the JavaDoc of the linked method, and that source should be considered
 * authoritative, rather than this one. That method also contains more information on the usage
 * of the message in question.
 *
 */
package net.navinet.research.deidentification.deidentify;