package com.github.synox.excel;


import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFHyperlink;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Hilfsklasse für einfaches erstellen von einfachen Sheets.
 * <p>
 * Example:
 * <code>
 * ExcelHelpers excel = new ExcelHelpers();
 * Row titleRow = excel.nextRow();
 * <p>
 * Row row = excel.nextRow();
 * int colNum = -1;
 * <p>
 * excel.addValue("Titel", "The foobar stories", row, ++colNum);
 * excel.setColumnWidth(colNum, 50 * 256);
 * <p>
 * excel.addValue("Kategorie", "Books and Movies", row, ++colNum);
 * excel.addValue("Zustand", "New", row, ++colNum);
 * <p>
 * excel.addTitleRow(titleRow);
 * <p>
 * excel.write("summary.xls");
 * </code>
 */
public class ExcelHelpers {
    private XSSFWorkbook workbook = new XSSFWorkbook();
    private final XSSFSheet sheet;

    private CellStyle currencyStyle = currenclyStyle(workbook);
    private CellStyle dateTimeStyle = dateTimeStyle(workbook);

    private Map<Integer, String> titles = new HashMap<>();

    private int rowPointer = 0;

    public ExcelHelpers() {
        sheet = workbook.createSheet("Sheet 1");
    }

    public void setColumnWidth(int colNum, int width) {
        sheet.setColumnWidth(colNum, width);
    }

    private Row createRow(int row) {
        return sheet.createRow(row);
    }

    public Row nextRow() {
        return createRow(rowPointer++);
    }


    public Cell addValue(String title, Object text, Row row, int colNum) {
        Cell cell = row.createCell(colNum);
        if (text != null) {
            cell.setCellValue(text.toString());
        }
        titles.put(colNum, title);
        return cell;
    }

    public Cell addValue(String title, Integer text, Row row, int colNum) {
        Cell cell = row.createCell(colNum);
        if (text != null) {
            cell.setCellValue(text);
        }
        titles.put(colNum, title);
        return cell;
    }

    public Cell addValue(String title, ZonedDateTime text, Row row, int colNum) {
        Cell cell = row.createCell(colNum);
        if (text != null) {
            cell.setCellValue(Date.from(text.toInstant()));
        }
        titles.put(colNum, title);
        cell.setCellStyle(dateTimeStyle);
        return cell;
    }

    public Cell addValue(String title, BigDecimal text, Row row, int colNum) {
        Cell cell = row.createCell(colNum);
        if (text != null) {
            cell.setCellValue(text.doubleValue());
        }
        titles.put(colNum, title);
        cell.setCellStyle(currencyStyle);
        return cell;
    }

    public Cell addValue(String title, boolean text, Row row, int colNum) {
        Cell cell = row.createCell(colNum);
        cell.setCellValue(text);
        titles.put(colNum, title);
        return cell;
    }

    private CellStyle currenclyStyle(XSSFWorkbook workbook) {
        CellStyle style = workbook.createCellStyle();
        DataFormat df = workbook.createDataFormat();
        style.setDataFormat(df.getFormat("#0"));
        return style;
    }

    private CellStyle dateTimeStyle(XSSFWorkbook workbook) {
        CellStyle style = workbook.createCellStyle();
        DataFormat df = workbook.createDataFormat();
        style.setDataFormat(df.getFormat("dd.mm.yy hh:mm"));
        return style;
    }


    public XSSFHyperlink createHyperlink(String url) {
        XSSFHyperlink link = workbook.getCreationHelper().createHyperlink(HyperlinkType.URL);
        link.setAddress(url);
        return link;
    }

    public void addTitleRow(Row titelRow) {
        titles.forEach((col, title) -> {
            Cell cell = titelRow.createCell(col);
            cell.setCellValue(title);
        });
        sheet.setAutoFilter(CellRangeAddress.valueOf("A1:Z1"));

    }

    public void write(String filename) throws IOException {
        try (FileOutputStream outputStream = new FileOutputStream(filename)) {
            workbook.write(outputStream);
        }
    }
}
