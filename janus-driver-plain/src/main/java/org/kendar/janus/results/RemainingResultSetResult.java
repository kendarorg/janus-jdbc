package org.kendar.janus.results;

import org.kendar.janus.serialization.TypedSerializer;

import java.util.List;

public class RemainingResultSetResult implements JdbcResult {
    public RemainingResultSetResult(){

    }

    public RemainingResultSetResult(boolean lastRow,List<List<Object>> rows){

        this.lastRow = lastRow;
        this.rows = rows;
    }
    private boolean lastRow;
    private List<List<Object>> rows;

    public void setLastRow(boolean lastRow) {
        this.lastRow = lastRow;
    }

    public boolean isLastRow() {
        return lastRow;
    }

    public void setRows(List<List<Object>> rows) {
        this.rows = rows;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("lastRow",lastRow);
        builder.write("rows",rows);
    }

    @Override
    public JdbcResult deserialize(TypedSerializer input) {
        return new RemainingResultSetResult(
                input.read("lastRow"),
                input.read("rows")
        );
    }
}
