package com.eagerlogic.entitydb;

/**
 *
 * @author dipacs
 */
public abstract class AFilterItem {
	
	protected AFilterItem() {}
    
    abstract String getCondition();
    abstract boolean match(Entity entity);

}
