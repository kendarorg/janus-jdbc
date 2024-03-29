package com.toddfast.util.convert;

import org.kendar.util.convert.TypeConverter.Convertible;
import org.kendar.util.convert.TypeConverter.Listener;
import org.kendar.util.convert.TypeConverter.Conversion;

/**
 *
 *
 */
public class TestSubclass extends TestSuperclass
		implements Listener, Convertible {

	public void beforeConversion(Object targetTypeKey) {
		System.out.println("--- beforeConversion("+targetTypeKey+")");
	}

	public Object afterConversion(Object targetTypeKey, Object convertedValue) {
		System.out.println("--- afterConversion("+
			targetTypeKey+","+convertedValue+")");
		return convertedValue;
	}

	public Conversion getTypeConversion(final Object targetTypeKey) {
		return new Conversion() {

			@Override
			public Object[] getTypeKeys() {
				return new Object[] { TestSubclass.class };
			}

			@Override
			public Object convert(Object value) {
				System.out.println("--- Converting value to type \""+key+
					"\": "+value);
				return "Converted Test value";
			}

			private final Object key=targetTypeKey;
		};
	}
}
