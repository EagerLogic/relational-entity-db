package com.eagerlogic.entitydb;

/**
 *
 * @author dipacs
 */
public final class BooleanFilterItem extends AFilterItem {
	
	private final String attributeName;
	private final boolean referenceValue;

	public BooleanFilterItem(String attributeName, boolean referenceValue) {
		if (attributeName == null) {
			throw new NullPointerException("The attributeName parameter can not be null.");
		}
		this.attributeName = attributeName;
		this.referenceValue = referenceValue;
	}

	public String getAttributeName() {
		return attributeName;
	}

	public boolean getReferenceValue() {
		return referenceValue;
	}

    @Override
    String getCondition() {
        return "(type=" + RelationalDB.AttributeDTO.EType.BOOLEAN.getType()
                + " AND name='" + attributeName.replace("'", "''") 
                + "' AND value='" + referenceValue + "')";
    }

    @Override
    boolean match(Entity entity) {
        Object attrO = entity.getAttribute(attributeName);
        if (attrO instanceof Boolean) {
            if (((Boolean)attrO) == referenceValue) {
                return true;
            }
        }
        return false;
    }

}
