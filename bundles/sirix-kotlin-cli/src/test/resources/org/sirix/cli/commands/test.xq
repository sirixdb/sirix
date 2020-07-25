let $nodeKey := sdb:nodekey(.=>foo[[2]])
let $xml :=
<xml>
    <bar>
        <hello>world</hello>
        <helloo>true</helloo>
    </bar>
    <baz>hello</baz>
    <foo>
        <element>bar</element>
        <element null="true"/>
        <element>2.33</element>
    </foo>
    <tada>
        <element>
            <foo>bar</foo>
        </element>
        <element>
            <baz>false</baz>
        </element>
        <element>boo</element>
        <element/>
        <element/>
    </tada>
</xml>
return {"nodeKey": $nodeKey}
