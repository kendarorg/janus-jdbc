package org.kendar.janus.cmd;

import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;
import org.kendar.janus.utils.ExceptionsWrapper;

import java.sql.SQLException;

public class Close implements JdbcCommand {
    private String initiator;

    public boolean isOnConnection() {
        return onConnection;
    }

    private boolean onConnection;

    public Close(){
        initiator=null;
    }

    public Close(Object initiator){

        this.initiator = initiator.getClass().getSimpleName();
        if(this.initiator.startsWith("Jdbc")){
            this.initiator = this.initiator.substring(4);
        }
    }


    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("onConnection",onConnection);

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {
        onConnection = input.read("onConnection");
        return this;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        try {
            if(onConnection){
                for(var item:context.values()){
                    try {
                        var method = item.getClass().getMethod("close");
                        method.invoke(item);
                    }catch (Exception ex){

                    }
                }
            }else {
                var item = context.get(uid);
                try {
                    var method = item.getClass().getMethod("close");
                    method.invoke(item);
                    context.remove(uid);
                } catch (Exception ex) {

                }
            }
        }catch (Exception ex){
            ExceptionsWrapper.toSQLException(ex);
        }
        return null;
    }

    @Override
    public String getPath() {
        return "/"+initiator+"/close";
    }

    @Override
    public String toString() {
        return "Close{}";
    }
}
