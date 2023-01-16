package org.kendar.janus.cmd.interfaces;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;

import java.util.List;

public interface JdbcPreparedStatementParameters {
    List<PreparedStatementParameter> getParameters();
}
