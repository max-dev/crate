package io.crate.planner.symbol;

import org.cratedb.DataType;

public class Value implements ValueSymbol {

    public static final SymbolFactory<Value> FACTORY = new SymbolFactory<Value>() {
        @Override
        public Value newInstance() {
            return new Value();
        }
    };

    private DataType type;

    public Value(DataType type) {
        this.type = type;
    }

    public Value() {

    }

    public DataType valueType() {
        return type;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.VALUE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitValue(this, context);
    }

}
