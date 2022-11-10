package org.kendar.janus.cmd;

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

    public RetrieveRemainingResultSet(){

    }

    public RetrieveRemainingResultSet(int columnCount, int maxRows) {

        this.columnCount = columnCount;
        this.maxRows = maxRows;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var result = new RemainingResultSetResult();
        var rows = new ArrayList<List<Object>>();
        var rs = (ResultSet)context.get(uid);
        var rowsCount = 0;
        var lastRow = false;
        while(rs.next()){

            var row = new ArrayList<Object>();
            for(var i=0;i<this.columnCount;i++){
                row.add(JdbcTypesConverter.convertToSerializable(rs.getObject(i+1)));
            }
            rows.add(row);
            rowsCount++;
            if(rowsCount>=maxRows){
                break;
            }

        }
        if(rowsCount==0){
            lastRow = true;
        }
        result.setLastRow(lastRow);
        result.setRows(rows);
        return result;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("columnCount",columnCount);
        builder.write("maxRows",maxRows);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {

        return new RetrieveRemainingResultSet(
                (int)input.read("columnCount"),
                (int)input.read("maxRows"));
    }
}
