package com.eagerlogic.entitydb;

/**
 *
 * @author dipacs
 */
public final class StringFilterItem extends AFilterItem {
	
	public static enum EOperator {
		SMALLER,
		GREATER,
		EQUALS,
		NOT_EQUALS,
		CONTAINS
	}
	
	private final String attributeName;
	private final EOperator operator;
	private final String referenceValue;

	public StringFilterItem(String attributeName, EOperator operator, String referenceValue) {
		if (attributeName == null) {
			throw new NullPointerException("The attributeName parameter can not be null.");
		}
		if (operator == null) {
			throw new NullPointerException("The operator parameter can not be null.");
		}
		this.attributeName = attributeName;
		this.operator = operator;
		if (referenceValue == null) {
			throw new NullPointerException("The referenceValue can not be null. Use NullFilterItem instead.");
		}
		this.referenceValue = referenceValue;
	}

	public String getAttributeName() {
		return attributeName;
	}

	public EOperator getOperator() {
		return operator;
	}

	public String getReferenceValue() {
		return referenceValue;
	}

    @Override
    String getCondition() {
        String res = "(type=" + RelationalDB.AttributeDTO.EType.STRING.getType()
                + " AND name='" + attributeName.replace("'", "''") 
                + "' AND ";
        
        if (operator == EOperator.CONTAINS) {
            res += "LOWER(value) LIKE '%" + referenceValue.toLowerCase().replace("'", "''").replace("%", "\\%") + "%'";
        } else if (operator == EOperator.EQUALS) {
            res += "value='" + referenceValue.replace("'", "''") + "'";
        } else if (operator == EOperator.GREATER) {
            res += "value>'" + referenceValue.replace("'", "''") + "'";
        } else if (operator == EOperator.NOT_EQUALS) {
            res += "value<>'" + referenceValue.replace("'", "''") + "'";
        } else if (operator == EOperator.SMALLER) {
            res += "value<'" + referenceValue.replace("'", "''") + "'";
        } else {
            throw new RuntimeException("Unknown StringFilterItem operator: " + operator.name());
        }
        res += ")";
        
        return res;
    }

    @Override
    boolean match(Entity entity) {
        Object o = entity.getAttribute(attributeName);
        if (!(o instanceof String)) {
            return false;
        }
        String s = (String) o;
        
        if (operator == EOperator.CONTAINS) {
            return s.toLowerCase().contains(referenceValue.toLowerCase());
        } else if (operator == EOperator.EQUALS) {
            return s.equals(referenceValue);
        } else if (operator == EOperator.GREATER) {
            return s.compareTo(referenceValue) > 0;
        } else if (operator == EOperator.NOT_EQUALS) {
            return !s.equals(referenceValue);
        } else if (operator == EOperator.SMALLER) {
            return s.compareTo(referenceValue) < 0;
        } else {
            throw new RuntimeException("Unknown StringFilterItem operator: " + operator.name());
        }
    }

}
