package org.kendar.janus.results;

import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.serialization.TypedSerializer;

public class ColumnDescriptor implements TypedSerializable {
    private int type;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    private String name;
    private String label;

    public ColumnDescriptor(){

    }
    public ColumnDescriptor(int type, String name, String label) {

        this.type = type;
        this.name = name;
        this.label = label;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("type",type);
        builder.write("name",name);
        builder.write("label",label);

    }

    @Override
    public Object deserialize(TypedSerializer input) {
        return new ColumnDescriptor(
                (int)input.read("type"),
                (String)input.read("name"),
                (String)input.read("label")
        );
    }
}
