package org.kendar.janus.serialization;

import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

public class GenerateMetadata {
    @Test
    public void test(){
        List<String> vars=new ArrayList<>();
        List<String> methods=new ArrayList<>();
        List<String> setup=new ArrayList<>();
        List<String> serialize=new ArrayList<>();
        List<String> deserialize=new ArrayList<>();
        for(var meth : DatabaseMetaData.class.getDeclaredMethods()){
            if(meth.getParameterTypes().length==0){
                vars.add("private "+meth.getReturnType()+" "+meth.getName()+";");
                methods.add("");
                methods.add("@Override");
                methods.add("public "+meth.getReturnType()+" "+meth.getName()+"(){ return "+meth.getName()+";}");
                setup.add(meth.getName()+"=src."+meth.getName()+"();");
//builder.write("sql",sql);
                deserialize.add("builder.write(\""+meth.getName()+"\","+meth.getName()+");");
                serialize.add(meth.getName()+" =builder.read(\""+meth.getName()+"\");");
            }
        }
        var svars = String.join("\n",vars);
        var smethods = String.join("\n",methods);
        var ssetup = String.join("\n",setup);
        var sdeserialize = String.join("\n",deserialize);
        var sserialize = String.join("\n",serialize);
        System.out.println("A");
    }
}

