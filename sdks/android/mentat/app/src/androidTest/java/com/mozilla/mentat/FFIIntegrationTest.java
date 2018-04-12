package com.mozilla.mentat;

import android.content.Context;
import android.content.res.AssetManager;
import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;
import android.util.Log;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Time;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Instrumentation test, which will execute on an Android device.
 *
 * @see <a href="http://d.android.com/tools/testing">Testing documentation</a>
 */
@RunWith(AndroidJUnit4.class)
public class FFIIntegrationTest {

    Mentat mentat = null;

    @Test
    public void useAppContext() throws Exception {
        // Context of the app under test.
        Context appContext = InstrumentationRegistry.getTargetContext();

        assertEquals("com.mozilla.mentat", appContext.getPackageName());
    }

    @Test
    public void openInMemoryStoreSucceeds() throws Exception {
        Mentat mentat = new Mentat();
        assertNotNull(mentat);
    }

    @Test
    public void openStoreInLocationSucceeds() throws Exception {
        Context context = InstrumentationRegistry.getTargetContext();
        String path = context.getDatabasePath("test.db").getAbsolutePath();
        Mentat mentat = new Mentat(path);
        assertNotNull(mentat);
    }

    public String readFile(String fileName) {
        Context testContext = InstrumentationRegistry.getInstrumentation().getContext();
        AssetManager assetManager = testContext.getAssets();
        try {
            InputStream inputStream = assetManager.open(fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder out = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line + "\n");
            }
            return out.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public TxReport transactCitiesSchema(Mentat mentat) {
        String citiesSchema = this.readFile("cities.schema");
        System.out.println(".t " + citiesSchema);
        return mentat.transact(citiesSchema);
    }

    public TxReport transactSeattleData(Mentat mentat) {
        String seattleData = this.readFile("all_seattle.edn");
        return mentat.transact(seattleData);
    }

    public Mentat getStore() {
        if (this.mentat == null) {
            this.mentat = new Mentat();
            this.transactCitiesSchema(mentat);
            this.transactSeattleData(mentat);
        }

        return this.mentat;
    }

    public TxReport populateWithTypesSchema(Mentat mentat) {
        String schema = "[\n" +
                "                [:db/add \"b\" :db/ident :foo/boolean]\n" +
                "                [:db/add \"b\" :db/valueType :db.type/boolean]\n" +
                "                [:db/add \"b\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"l\" :db/ident :foo/long]\n" +
                "                [:db/add \"l\" :db/valueType :db.type/long]\n" +
                "                [:db/add \"l\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"r\" :db/ident :foo/ref]\n" +
                "                [:db/add \"r\" :db/valueType :db.type/ref]\n" +
                "                [:db/add \"r\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"i\" :db/ident :foo/instant]\n" +
                "                [:db/add \"i\" :db/valueType :db.type/instant]\n" +
                "                [:db/add \"i\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"d\" :db/ident :foo/double]\n" +
                "                [:db/add \"d\" :db/valueType :db.type/double]\n" +
                "                [:db/add \"d\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"s\" :db/ident :foo/string]\n" +
                "                [:db/add \"s\" :db/valueType :db.type/string]\n" +
                "                [:db/add \"s\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"k\" :db/ident :foo/keyword]\n" +
                "                [:db/add \"k\" :db/valueType :db.type/keyword]\n" +
                "                [:db/add \"k\" :db/cardinality :db.cardinality/one]\n" +
                "                [:db/add \"u\" :db/ident :foo/uuid]\n" +
                "                [:db/add \"u\" :db/valueType :db.type/uuid]\n" +
                "                [:db/add \"u\" :db/cardinality :db.cardinality/one]\n" +
                "            ]";
        TxReport report = mentat.transact(schema);
        Long stringEntid = report.getEntidForTempId("s");

        String data = "[\n" +
                "                [:db/add \"a\" :foo/boolean true]\n" +
                "                [:db/add \"a\" :foo/long 25]\n" +
                "                [:db/add \"a\" :foo/instant #inst \"2017-01-01T11:00:00.000Z\"]\n" +
                "                [:db/add \"a\" :foo/double 11.23]\n" +
                "                [:db/add \"a\" :foo/string \"The higher we soar the smaller we appear to those who cannot fly.\"]\n" +
                "                [:db/add \"a\" :foo/keyword :foo/string]\n" +
                "                [:db/add \"a\" :foo/uuid #uuid \"550e8400-e29b-41d4-a716-446655440000\"]\n" +
                "                [:db/add \"b\" :foo/boolean false]\n" +
                "                [:db/add \"b\" :foo/ref "+ stringEntid +"]\n" +
                "                [:db/add \"b\" :foo/long 50]\n" +
                "                [:db/add \"b\" :foo/instant #inst \"2018-01-01T11:00:00.000Z\"]\n" +
                "                [:db/add \"b\" :foo/double 22.46]\n" +
                "                [:db/add \"b\" :foo/string \"Silence is worse; all truths that are kept silent become poisonous.\"]\n" +
                "                [:db/add \"b\" :foo/uuid #uuid \"4cb3f828-752d-497a-90c9-b1fd516d5644\"]\n" +
                "            ]";
        return mentat.transact(data);
    }

    @Test
    public void transactingVocabularySucceeds() {
        Mentat mentat = new Mentat();
        TxReport schemaReport = this.transactCitiesSchema(mentat);
        assertNotNull(schemaReport);
        assertTrue(schemaReport.getTxId() > 0);
    }

    @Test
    public void transactingEntitiesSucceeds() {
        Mentat mentat = new Mentat();
        this.transactCitiesSchema(mentat);
        TxReport dataReport = this.transactSeattleData(mentat);
        assertNotNull(dataReport);
        assertTrue(dataReport.getTxId() > 0);
        Long entid = dataReport.getEntidForTempId("a17592186045438");
        assertEquals(65566, entid.longValue());
    }

    @Test
    public void executeScalarSucceeds() throws InterruptedException {
        Mentat mentat = getStore();
        String query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindString("?name", "Wallingford").executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals("KOMO Communities - Wallingford", value.asString());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void executeCollSucceeds() throws InterruptedException {
        Mentat mentat = getStore();
        String query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]";
        final Expectation expectation = new Expectation();
        mentat.query(query).executeColl(new CollResultHandler() {
            @Override
            public void handleList(CollResult list) {
                assertNotNull(list);
                for (int i = 0; i < 3; ++i) {
                    assertNotNull(list.asDate(i));
                }
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void executeCollResultIteratorSucceeds() throws InterruptedException {
        Mentat mentat = getStore();
        String query = "[:find [?when ...] :where [_ :db/txInstant ?when] :order (asc ?when)]";
        final Expectation expectation = new Expectation();
        mentat.query(query).executeColl(new CollResultHandler() {
            @Override
            public void handleList(CollResult list) {
                assertNotNull(list);

                for(TypedValue value: list) {
                    assertNotNull(value.asDate());
                }
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void executeTupleSucceeds() throws InterruptedException {
        Mentat mentat = getStore();
        String query = "[:find [?name ?cat]\n" +
                "        :where\n" +
                "        [?c :community/name ?name]\n" +
                "        [?c :community/type :community.type/website]\n" +
                "        [(fulltext $ :community/category \"food\") [[?c ?cat]]]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).executeTuple(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                String name = row.asString(0);
                String category = row.asString(1);
                assert(name == "Community Harvest of Southwest Seattle");
                assert(category == "sustainable food");
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void executeRelIteratorSucceeds() throws InterruptedException {
        Mentat mentat = getStore();
        String query = "[:find ?name ?cat\n" +
                "        :where\n" +
                "        [?c :community/name ?name]\n" +
                "        [?c :community/type :community.type/website]\n" +
                "        [(fulltext $ :community/category \"food\") [[?c ?cat]]]]";

        final LinkedHashMap expectedResults = new LinkedHashMap<String, String>();
        expectedResults.put("InBallard", "food");
        expectedResults.put("Seattle Chinatown Guide", "food");
        expectedResults.put("Community Harvest of Southwest Seattle", "sustainable food");
        expectedResults.put("University District Food Bank", "food bank");
        final Expectation expectation = new Expectation();
        mentat.query(query).execute(new RelResultHandler() {
            @Override
            public void handleRows(RelResult rows) {
                assertNotNull(rows);
                int index = 0;
                for (TupleResult row: rows) {
                    String name = row.asString(0);
                    assertNotNull(name);
                    String category = row.asString(1);
                    assertNotNull(category);
                    String expectedCategory = expectedResults.get(name).toString();
                    assertNotNull(expectedCategory);
                    assertEquals(expectedCategory, category);
                    ++index;
                }
                assertEquals(expectedResults.size(), index);
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void executeRelSucceeds() throws InterruptedException {
        Mentat mentat = getStore();
        String query = "[:find ?name ?cat\n" +
                "        :where\n" +
                "        [?c :community/name ?name]\n" +
                "        [?c :community/type :community.type/website]\n" +
                "        [(fulltext $ :community/category \"food\") [[?c ?cat]]]]";

        final LinkedHashMap expectedResults = new LinkedHashMap<String, String>();
        expectedResults.put("InBallard", "food");
        expectedResults.put("Seattle Chinatown Guide", "food");
        expectedResults.put("Community Harvest of Southwest Seattle", "sustainable food");
        expectedResults.put("University District Food Bank", "food bank");
        final Expectation expectation = new Expectation();
        mentat.query(query).execute(new RelResultHandler() {
            @Override
            public void handleRows(RelResult rows) {
                assertNotNull(rows);
                for (int i = 0; i < expectedResults.size(); ++i) {
                    TupleResult row = rows.rowAtIndex(i);
                    assertNotNull(row);
                    String name = row.asString(0);
                    assertNotNull(name);
                    String category = row.asString(1);
                    assertNotNull(category);
                    String expectedCategory = expectedResults.get(name).toString();
                    assertNotNull(expectedCategory);
                    assertEquals(expectedCategory, category);
                }
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingLongValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?long :where [?e :foo/long ?long]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindLong("?long", 25).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingRefValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        long stringEntid = mentat.entIdForAttribute(":foo/string");
        final Long bEntid = report.getEntidForTempId("b");
        String query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?ref", stringEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(bEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingRefKwValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        String refKeyword = ":foo/string";
        final Long bEntid = report.getEntidForTempId("b");
        String query = "[:find ?e . :in ?ref :where [?e :foo/ref ?ref]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindKeywordReference("?ref", refKeyword).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(bEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingKwValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?kw :where [?e :foo/keyword ?kw]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindKeyword("?kw", ":foo/string").executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingDateValueSucceeds() throws InterruptedException, ParseException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ", Locale.ENGLISH);
        format.parse("2018-04-16T16:39:18+00:00");
        Calendar cal = format.getCalendar();
        String query = "[:find [?e ?d] :in ?now :where [?e :foo/instant ?d] [(< ?d ?now)]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindDate("?now", cal.getTime()).executeTuple(new TupleResultHandler() {
            @Override
            public void handleRow(TupleResult row) {
                assertNotNull(row);
                TypedValue value = row.get(0);
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingStringValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        String query = "[:find ?n . :in ?name :where [(fulltext $ :community/name ?name) [[?e ?n]]]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindString("?name", "Wallingford").executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals("\"KOMO Communities - Wallingford\"", value.asString());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingUuidValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?uuid :where [?e :foo/uuid ?uuid]]";
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        final Expectation expectation = new Expectation();
        mentat.query(query).bindUUID("?uuid", uuid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingBooleanValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?bool :where [?e :foo/boolean ?bool]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindBoolean("?bool", true).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });

        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void bindingDoubleValueSucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :in ?double :where [?e :foo/double ?double]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindDouble("?double", 11.23).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToLong() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/long ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(25, value.asLong().longValue());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToRef() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?e . :where [?e :foo/long 25]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(aEntid, value.asEntid());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToKeyword() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/keyword ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(":foo/string", value.asKeyword());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToBoolean() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/boolean ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(true, value.asBoolean());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToDouble() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/double ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(new Double(11.23), value.asDouble());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToDate() throws InterruptedException, ParseException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/instant ?v]]";
        final Expectation expectation = new Expectation();
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ", Locale.ENGLISH);
        format.parse("2017-01-01T11:00:00+00:00");
        final Calendar expectedDate = format.getCalendar();
        mentat.query(query).bindEntidReference("?e", aEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(expectedDate.getTime(), value.asDate());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToString() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/string ?v]]";
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals("The higher we soar the smaller we appear to those who cannot fly.", value.asString());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void typedValueConvertsToUUID() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String query = "[:find ?v . :in ?e :where [?e :foo/uuid ?v]]";
        final UUID expectedUUID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        final Expectation expectation = new Expectation();
        mentat.query(query).bindEntidReference("?e", aEntid).executeScalar(new ScalarResultHandler() {
            @Override
            public void handleValue(TypedValue value) {
                assertNotNull(value);
                assertEquals(expectedUUID, value.asUUID());
                expectation.fulfill();
            }
        });
        synchronized (expectation) {
            expectation.wait(1000);
        }
        assertTrue(expectation.isFulfilled);
    }

    @Test
    public void valueForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        TypedValue value = mentat.valueForAttributeOfEntity(":foo/long", aEntid);
        assertNotNull(value);
        assertEquals(25, value.asLong().longValue());
    }

    @Test
    public void entidForAttributeSucceeds() {
        Mentat mentat = new Mentat();
        this.populateWithTypesSchema(mentat);
        long entid = mentat.entIdForAttribute(":foo/long");
        assertEquals(65540, entid);
    }

    @Test
    public void setLongForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String attr = ":foo/long";
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(25, pre.asLong().longValue());

        mentat.setLongForAttributeOfEntity(100, attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(100, post.asLong().longValue());
    }

    @Test
    public void setBooleanForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String attr = ":foo/boolean";
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(true, pre.asBoolean());

        mentat.setBooleanForAttributeOfEntity(false, attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(false, post.asBoolean());
    }

    @Test
    public void setRefForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        final Long bEntid = report.getEntidForTempId("b");
        String attr = ":foo/ref";
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertNull(pre);

        mentat.setReferenceForAttributeOfEntity(bEntid, attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(bEntid, post.asEntid());
    }

    @Test
    public void setRefKwForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String attr = ":foo/ref";
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertNull(pre);

        mentat.setKeywordReferenceForAttributeOfEntity(":foo/long", attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(65540, post.asEntid().longValue());
    }

    @Test
    public void setDateForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");

        final Date previousDate = new Date(1483268400000L);

        String attr = ":foo/instant";
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertNotNull(pre);
        Date preDate = pre.asDate();
        assertEquals(previousDate, preDate);

        Date newDate = new Date(1523973302000L);
        mentat.setDateForAttributeOfEntity(newDate, attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        Date postDate = post.asDate();
        assertEquals(newDate, postDate);
    }

    @Test
    public void setDoubleForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String attr = ":foo/double";
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(new Double(11.23), pre.asDouble());

        mentat.setDoubleForAttributeOfEntity(22.0, attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(new Double(22.0), post.asDouble());
    }

    @Test
    public void setStringForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String attr = ":foo/string";
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals("The higher we soar the smaller we appear to those who cannot fly.", pre.asString());

        mentat.setStringForAttributeOfEntity("Become who you are!", attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals("Become who you are!", post.asString());
    }

    @Test
    public void setUUIDForAttributeOfEntitySucceeds() throws InterruptedException {
        Mentat mentat = new Mentat();
        TxReport report = this.populateWithTypesSchema(mentat);
        final Long aEntid = report.getEntidForTempId("a");
        String attr = ":foo/uuid";
        UUID previousUUID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        TypedValue pre = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(previousUUID, pre.asUUID());

        UUID newUUID = UUID.randomUUID();
        mentat.setUUIDForAttributeOfEntity(newUUID, attr, aEntid);

        TypedValue post = mentat.valueForAttributeOfEntity(attr, aEntid);
        assertEquals(newUUID, post.asUUID());
    }
}
