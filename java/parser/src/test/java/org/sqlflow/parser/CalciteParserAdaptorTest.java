package org.sqlflow.parser;

import static org.junit.Assert.*;

import java.util.ArrayList;
import org.junit.Test;

public class CalciteParserAdaptorTest {
  private ArrayList<String> tests(int random) {
    ArrayList<String> standard_select = new ArrayList<String>();
    standard_select.add(String.format("select %d", random));
    standard_select.add(String.format("select *, %d from my_table", random));
    standard_select.add(String.format(
        "SELECT %d\n"
            + "customerNumber,  \n"
            + "    customerName \n"
            + "FROM \n"
            + "    customers \n"
            + "WHERE \n"
            + "    EXISTS( SELECT  \n"
            + "            orderNumber, SUM(priceEach * quantityOrdered) \n"
            + "        FROM \n"
            + "            orderdetails \n"
            + "                INNER JOIN \n"
            + "            orders USING (orderNumber) \n"
            + "        WHERE \n"
            + "            customerNumber = customers.customerNumber \n"
            + "        GROUP BY orderNumber \n"
            + "        HAVING SUM(priceEach * quantityOrdered) > 60000)", random));
    return standard_select;
  }

  @Test
  public void testParseAndSplit() {
    ArrayList<String> standard_select = tests(1);

    // one standard SQL statement
    for (String sql : standard_select) {
      String sql_program = String.format("%s;", sql);
      ParseResult parse_result = (new CalciteParserAdaptor()).ParseAndSplit(sql_program);
      assertEquals(-1, parse_result.Position);
      assertEquals("", parse_result.Error);
      assertEquals(1, parse_result.Statements.size());
      assertEquals(sql, parse_result.Statements.get(0));
    }

    // two standard SQL statements
    for (String sql : standard_select) {
      String sql_program = String.format("%s;%s;", sql, sql);
      ParseResult parse_result = (new CalciteParserAdaptor()).ParseAndSplit(sql_program);
      assertEquals(-1, parse_result.Position);
      assertEquals("", parse_result.Error);
      assertEquals(2, parse_result.Statements.size());
      assertEquals(sql, parse_result.Statements.get(0));
      assertEquals(sql, parse_result.Statements.get(1));
    }

    // two SQL statements, the first one is extendedSQL
    for (String sql : standard_select) {
      String sql_program = String.format("%s to train;%s;", sql, sql);
      ParseResult parse_result = (new CalciteParserAdaptor()).ParseAndSplit(sql_program);
      assertEquals(sql.length() + 1, parse_result.Position);
      assertEquals("", parse_result.Error);
      assertEquals(1, parse_result.Statements.size());
      assertEquals(sql + " ", parse_result.Statements.get(0));
    }

    // two SQL statements, the second one is extendedSQL
    for (String sql : standard_select) {
      String sql_program = String.format("%s;%s to train;", sql, sql);
      ParseResult parse_result = (new CalciteParserAdaptor()).ParseAndSplit(sql_program);
      assertEquals(sql.length() + 1 + sql.length() + 1, parse_result.Position);
      assertEquals("", parse_result.Error);
      assertEquals(2, parse_result.Statements.size());
      assertEquals(sql, parse_result.Statements.get(0));
      assertEquals(sql + " ", parse_result.Statements.get(1));
    }

    { // two SQL statements, the first standard SQL has an error.
      String sql_program = "select select 1; select 1 to train;";
      ParseResult parse_result = (new CalciteParserAdaptor()).ParseAndSplit(sql_program);
      assertEquals(0, parse_result.Statements.size());
      assertEquals(-1, parse_result.Position);
      assertTrue(parse_result.Error.startsWith("Encountered \"select\" at line 1, column 8."));
    }

    // two SQL statements, the second standard SQL has an error.
    for (String sql : standard_select) {
      String sql_program = String.format("%s;select select 1;", sql);
      ParseResult parse_result = (new CalciteParserAdaptor()).ParseAndSplit(sql_program);
      assertEquals(0, parse_result.Statements.size());
      assertEquals(-1, parse_result.Position);
      assertTrue(parse_result.Error.startsWith("Encountered \"select\" at line 1, column 8."));
    }

    { // non select statement before to train
      String sql_program = "describe table to train;";
      ParseResult parse_result = (new CalciteParserAdaptor()).ParseAndSplit(sql_program);
      assertEquals(0, parse_result.Statements.size());
      assertEquals(-1, parse_result.Position);
      assertTrue(parse_result.Error.startsWith("Encountered \"to\" at line 1, column 16."));
    }
  }

  @Test
  public void testParseAndSplitPerf() {
    int count = 10000;
    CalciteParserAdaptor parser = new CalciteParserAdaptor();
    long started = System.currentTimeMillis();
    for (int i=0; i<count; ++i) {
      for (String sql : tests(i)) {
        String sql_program = String.format("%s;", sql);
        ParseResult pr = parser.ParseAndSplit(sql_program);
        assertEquals("", pr.Error);
      }
    }
    long costs = System.currentTimeMillis() - started;
    // #count*tests(0).size() vs costs millisecond
    // 10000 * 3 -> 16253
    // 1000 * 3 -> 4576
    System.out.println(costs);
  }
}
