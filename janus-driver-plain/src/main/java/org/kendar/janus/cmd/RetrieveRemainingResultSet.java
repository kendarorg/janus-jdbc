package org.kendar.janus.cmd;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.results.RemainingResultSetResult;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;
import org.kendar.janus.utils.JdbcTypesConverter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RetrieveRemainingResultSet implements JdbcCommand {

    private int columnCount;
    private int maxRows;
    private boolean currentRow;

    public RetrieveRemainingResultSet(){

    }

    public RetrieveRemainingResultSet(int columnCount, int maxRows,boolean currentRow) {

        this.columnCount = columnCount;
        this.maxRows = maxRows;
        this.currentRow = currentRow;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var result = new RemainingResultSetResult();
        var rows = new ArrayList<List<Object>>();
        var rs = (ResultSet)context.get(uid);
        var rowsCount = 0;
        var lastRow = false;
        if(currentRow){
            var row = new ArrayList<>();
            for(var i=0;i<this.columnCount;i++){
                row.add(JdbcTypesConverter.convertToSerializable(rs.getObject(i+1)));
            }
            rows.add(row);
            rowsCount++;
            lastRow = false;
        }else {
            while (rs.next()) {

                var row = new ArrayList<>();
                for (var i = 0; i < this.columnCount; i++) {
                    row.add(JdbcTypesConverter.convertToSerializable(rs.getObject(i + 1)));
                }
                rows.add(row);
                rowsCount++;
                if (rowsCount >= maxRows) {
                    break;
                }

            }
            if (rowsCount == 0) {
                lastRow = true;
            }
        }
        result.setLastRow(lastRow);
        result.setRows(rows);
        return result;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("columnCount",columnCount);
        builder.write("maxRows",maxRows);
        builder.write("currentRow",currentRow);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {

        columnCount =input.read("columnCount");
        maxRows = input.read("maxRows");
        currentRow = input.read("currentRow");
        return this;
    }

    @Override
    public String toString() {
        return "RetrieveRemainingResultSet{" +
                "\n\tcolumnCount=" + columnCount +
                ", \n\tmaxRows=" + maxRows +
                ", \n\tcurrentRow=" + currentRow +
                '}';
    }


    @Override
    public String getPath() {
        return "/ResultSet/getRemainingResultSet";
    }
}
