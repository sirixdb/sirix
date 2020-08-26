package org.sirix.index.art;

/**
 * For using {@link AdaptiveRadixTree}, the keys need to be transformed into binary comparable keys
 * which are the byte array representation of your keys such that the result of doing
 * lexicographic comparison over them is the same as doing the key comparison.
 *
 * <h2>Example of key transformation</h2>
 * <h3>Signed integers</h3>
 * Signed integers are stored in two's complement notation.
 * This means that negative integers always have their MSB set and hence are
 * bitwise lexicographically greater than positive integers.
 * <p>
 * For example -1 in 2's complement form is 1111 1111 1111 1111 1111 1111 1111 1111,
 * whereas +1 is 0000 0000 0000 0000 0000 0000 0000 0001.
 * <p>
 * This is not the correct binary comparable transformation since
 * +1 &gt; -1 but the above transformation lexicographically orders +1 before -1.
 * <p>
 * In this case, the right transformation is obtained by flipping the sign bit.
 * <p>
 * Therefore -1 will be 0111 1111 1111 1111 1111 1111 1111 1111 and +1 as 1000 0000 0000 0000 0000 0000 0000 0001.
 *
 * <h3>ASCII encoded character strings</h3>
 * Naturally yield the expected order as 'a' &lt; 'b' and their respective byte values 97 &lt; 98 obey the order.
 *
 * <h3>IPv4 addresses</h3>
 * Naturally yield the expected order since each octet is an unsigned byte and unsigned types in binary have the expected lexicographic ordering.
 * <p>
 * For example, 12.10.192.0 &lt; 12.10.199.255 and their respective binary representation 00001100.00001010.11000000.00000000 is lexicographically smaller than 00001100.00001010.11000111.11111111.
 *
 * <h2>Implementing the interface</h2>
 * <h3>Simple keys based on primitives and String</h3>
 * {@link BinaryComparables} already provides the key transformations for primitives and Strings.
 *
 * <h3>Compound keys</h3>
 * <h4>With only fixed length attributes</h4>
 * Transform each attribute separately and concatenate the results.
 * <p>
 * This example shows the transformation for a compound key made up of two integers.
 *
 * <h4>With variable length attributes</h4>
 * Transformation of a variable length attribute that is succeeded by another attribute is required to end with a byte 0 for the right transformation. Without it, compound key ("a", "bc") and ("ab", "c") would be incorrectly treated equal. Note this only works if byte 0 is not part of the variable length attribute's key space, otherwise ("a\0", "b") would be incorrectly ordered before ("a", "b").
 * <p>
 * If byte 0 is part of the key space then the key transformation requires remapping every byte 0 as byte 0 followed by byte 1 and ending with two byte 0s. This is described in section IV.B (e).
 *
 * <h2>Further reading</h2>
 * Section IV of the paper.
 *
 * @param <K> the key type to be used in {@link AdaptiveRadixTree}
 * @see BinaryComparables Implementation of this interface for primitives and String.
 */
public interface BinaryComparable<K> {
  byte[] get(K key);
}

