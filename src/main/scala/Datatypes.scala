object Datatypes {

  /*
Datatype	Range
Byte	integers in the range from −128 to 127 ( 0 t0 8 bits)
Short	integers in the range from −32768 to 32767 (0 to 16 bits )
Int	integers in the range from −2147483648 to 2147483647 (0 to 32 bits )
Long	integers in the range from −9223372036854775808 to9223372036854775807 (0 to 64 bits )
Float	largest positive finite float: 3.4028235×1038 and smallest positive finite nonzero float: 1.40×10−45
Double	largest positive finite double:1.7976931348623157×10308 and the smallest positive finite nonzero double: 4.9×10−324
char	Unicode characters with code points in the range from U+0000to U+FFFF
String	a finite sequence of Chars
Boolean	either the literal true or the literal false
Unit	either the literal true or the literal false
Null	null or empty reference
Nothing	the subtype of every other type; includes no values
Any	Any the supertype of any type; any object is of type Any
AnyRef	The supertype of any reference type i.e. nonvalue Scala classes and user-defined classes

   */

  var a: Boolean = true
  var a1: Byte = 126
  var a2: Float = 2.45673f
  var a3: Int = 3
  var a4: Short = 45
  var a5: Double = 2.93846523
  var a6: Char = 'A'
  if (a == true) {
    println("boolean:nishanth")
  }
  println("byte:" + a1)
  println("float:" + a2)
  println("integer:" + a3)
  println("short:" + a4)
  println("double:" + a5)
  println("char:" + a6)

}


/*
Escape Sequences
The following escape sequences are recognized in character and string literals.

Escape Sequences	Unicode	Description
    \b	\u0008	backspace BS
    \t	\u0009	horizontal tab HT
    \n	\u000c	formfeed FF
    \f	\u000c	formfeed FF
    \r	\u000d	carriage return CR
    \"	\u0022	double quote "
    \'	\u0027	single quote .
    \\	\u005c	backslash \

 */