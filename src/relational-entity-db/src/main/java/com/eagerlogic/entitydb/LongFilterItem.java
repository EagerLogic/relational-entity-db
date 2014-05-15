package com.eagerlogic.entitydb;

/**
 *
 * @author dipacs
 */
public final class LongFilterItem extends AFilterItem {

    public static enum EOperator {

        SMALLER,
        GREATER,
        EQUALS,
        NOT_EQUALS
    }

    private final String attributeName;
    private final EOperator operator;
    private final long referenceValue;

    public LongFilterItem(String attributeName, EOperator operator, long referenceValue) {
        if (attributeName == null) {
            throw new NullPointerException("The attributeName parameter can not be null.");
        }
        if (operator == null) {
            throw new NullPointerException("The operator parameter can not be null.");
        }
        this.attributeName = attributeName;
        this.operator = operator;
        this.referenceValue = referenceValue;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public EOperator getOperator() {
        return operator;
    }

    public long getReferenceValue() {
        return referenceValue;
    }

    @Override
    String getCondition() {
        String res = "(type=" + RelationalDB.AttributeDTO.EType.LONG.getType()
                + " AND name='" + attributeName.replace("'", "''")
                + "' AND value";

        if (operator == EOperator.EQUALS) {
            res += "=";
        } else if (operator == EOperator.GREATER) {
            res += " IS NOT NULL )";
            return res;
        } else if (operator == EOperator.SMALLER) {
            res += " IS NOT NULL )";
            return res;
        } else if (operator == EOperator.NOT_EQUALS) {
            res += "<>";
        } else {
            throw new RuntimeException("Unknown LongFilterItem operator: " + operator.name());
        }

        res += "'" + referenceValue + "')";
        return res;
    }

    @Override
    boolean match(Entity entity) {
        Object attrO = entity.getAttribute(attributeName);
        if (attrO instanceof Long) {
            long value = (Long) attrO;
            if (operator == EOperator.EQUALS) {
                return value == referenceValue;
            } else if (operator == EOperator.GREATER) {
                return value > referenceValue;
            } else if (operator == EOperator.SMALLER) {
                return value < referenceValue;
            } else if (operator == EOperator.NOT_EQUALS) {
                return value != referenceValue;
            } else {
                throw new RuntimeException("Unknown LongFilterItem operator: " + operator.name());
            }
        }
        return false;
    }

}
