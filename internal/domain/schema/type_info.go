// SPDX-License-Identifier: Apache-2.0

package schema

import "strconv"

// valueKind selects the conversion applied to a value's text form.
type valueKind uint8

const (
	kindText valueKind = iota
	kindInt
	kindFloat
	kindBool
	kindBytea
	kindJSON
	kindDate
	kindTime
	kindTimeTZ
	kindTimestamp
	kindTimestampTZ
	kindInterval
	kindArray
	kindRange
	kindMultirange
)

// typeInfo describes one built-in type of the mapping table. elem is the
// element type of arrays, ranges (bound type) and multiranges (range type);
// delim is the array element delimiter (pg_type.typdelim of the element).
type typeInfo struct {
	name  string
	kind  valueKind
	elem  uint32
	delim byte
}

func scalar(name string, kind valueKind) typeInfo { return typeInfo{name: name, kind: kind} }

func arrayOf(name string, elem uint32) typeInfo {
	return typeInfo{name: name, kind: kindArray, elem: elem, delim: ','}
}

// builtinTypes is the constant mapping table (see the package documentation).
var builtinTypes = map[uint32]typeInfo{
	OIDInt2: scalar("int2", kindInt), OIDInt4: scalar("int4", kindInt), OIDInt8: scalar("int8", kindInt),
	OIDOID: scalar("oid", kindInt), OIDXID: scalar("xid", kindInt), OIDCID: scalar("cid", kindInt),
	OIDXID8: scalar("xid8", kindInt),

	OIDFloat4: scalar("float4", kindFloat), OIDFloat8: scalar("float8", kindFloat),
	OIDBool:  scalar("bool", kindBool),
	OIDBytea: scalar("bytea", kindBytea),
	OIDJSON:  scalar("json", kindJSON), OIDJSONB: scalar("jsonb", kindJSON),

	OIDDate: scalar("date", kindDate), OIDTime: scalar("time", kindTime), OIDTimeTZ: scalar("timetz", kindTimeTZ),
	OIDTimestamp: scalar("timestamp", kindTimestamp), OIDTimestampTZ: scalar("timestamptz", kindTimestampTZ),
	OIDInterval: scalar("interval", kindInterval),

	OIDNumeric: scalar("numeric", kindText), OIDMoney: scalar("money", kindText),
	OIDText: scalar("text", kindText), OIDVarchar: scalar("varchar", kindText), OIDBPChar: scalar("bpchar", kindText),
	OIDName: scalar("name", kindText), OIDChar: scalar("char", kindText), OIDUUID: scalar("uuid", kindText),
	OIDXML: scalar("xml", kindText), OIDJSONPath: scalar("jsonpath", kindText),
	OIDInet: scalar("inet", kindText), OIDCIDR: scalar("cidr", kindText),
	OIDMacaddr: scalar("macaddr", kindText), OIDMacaddr8: scalar("macaddr8", kindText),
	OIDBit: scalar("bit", kindText), OIDVarbit: scalar("varbit", kindText),
	OIDPoint: scalar("point", kindText), OIDLseg: scalar("lseg", kindText), OIDPath: scalar("path", kindText),
	OIDBox: scalar("box", kindText), OIDPolygon: scalar("polygon", kindText), OIDLine: scalar("line", kindText),
	OIDCircle: scalar("circle", kindText), OIDTSVector: scalar("tsvector", kindText),
	OIDTSQuery: scalar("tsquery", kindText), OIDPgLSN: scalar("pg_lsn", kindText),
	OIDTID: scalar("tid", kindText), OIDInt2Vector: scalar("int2vector", kindText),
	OIDOIDVector: scalar("oidvector", kindText), OIDTxidSnapshot: scalar("txid_snapshot", kindText),
	OIDPgSnapshot: scalar("pg_snapshot", kindText), OIDACLItem: scalar("aclitem", kindText),
	OIDRefcursor: scalar("refcursor", kindText), OIDRegProc: scalar("regproc", kindText),
	OIDRegProcedure: scalar("regprocedure", kindText), OIDRegOper: scalar("regoper", kindText),
	OIDRegOperator: scalar("regoperator", kindText), OIDRegClass: scalar("regclass", kindText),
	OIDRegType: scalar("regtype", kindText), OIDRegConfig: scalar("regconfig", kindText),
	OIDRegDictionary: scalar("regdictionary", kindText), OIDRegNamespace: scalar("regnamespace", kindText),
	OIDRegRole: scalar("regrole", kindText), OIDRegCollation: scalar("regcollation", kindText),

	OIDInt4Range: {name: "int4range", kind: kindRange, elem: OIDInt4},
	OIDInt8Range: {name: "int8range", kind: kindRange, elem: OIDInt8},
	OIDNumRange:  {name: "numrange", kind: kindRange, elem: OIDNumeric},
	OIDDateRange: {name: "daterange", kind: kindRange, elem: OIDDate},
	OIDTSRange:   {name: "tsrange", kind: kindRange, elem: OIDTimestamp},
	OIDTSTZRange: {name: "tstzrange", kind: kindRange, elem: OIDTimestampTZ},

	OIDInt4Multirange: {name: "int4multirange", kind: kindMultirange, elem: OIDInt4Range},
	OIDInt8Multirange: {name: "int8multirange", kind: kindMultirange, elem: OIDInt8Range},
	OIDNumMultirange:  {name: "nummultirange", kind: kindMultirange, elem: OIDNumRange},
	OIDDateMultirange: {name: "datemultirange", kind: kindMultirange, elem: OIDDateRange},
	OIDTSMultirange:   {name: "tsmultirange", kind: kindMultirange, elem: OIDTSRange},
	OIDTSTZMultirange: {name: "tstzmultirange", kind: kindMultirange, elem: OIDTSTZRange},

	OIDBoolArray: arrayOf("bool[]", OIDBool), OIDByteaArray: arrayOf("bytea[]", OIDBytea),
	OIDCharArray: arrayOf("char[]", OIDChar), OIDNameArray: arrayOf("name[]", OIDName),
	OIDInt2Array: arrayOf("int2[]", OIDInt2), OIDInt2VectorArray: arrayOf("int2vector[]", OIDInt2Vector),
	OIDInt4Array: arrayOf("int4[]", OIDInt4), OIDRegProcArray: arrayOf("regproc[]", OIDRegProc),
	OIDTextArray: arrayOf("text[]", OIDText), OIDTIDArray: arrayOf("tid[]", OIDTID),
	OIDXIDArray: arrayOf("xid[]", OIDXID), OIDCIDArray: arrayOf("cid[]", OIDCID),
	OIDOIDVectorArray: arrayOf("oidvector[]", OIDOIDVector), OIDBPCharArray: arrayOf("bpchar[]", OIDBPChar),
	OIDVarcharArray: arrayOf("varchar[]", OIDVarchar), OIDInt8Array: arrayOf("int8[]", OIDInt8),
	OIDPointArray: arrayOf("point[]", OIDPoint), OIDLsegArray: arrayOf("lseg[]", OIDLseg),
	OIDPathArray:   arrayOf("path[]", OIDPath),
	OIDBoxArray:    {name: "box[]", kind: kindArray, elem: OIDBox, delim: ';'},
	OIDFloat4Array: arrayOf("float4[]", OIDFloat4), OIDFloat8Array: arrayOf("float8[]", OIDFloat8),
	OIDPolygonArray: arrayOf("polygon[]", OIDPolygon), OIDOIDArray: arrayOf("oid[]", OIDOID),
	OIDACLItemArray: arrayOf("aclitem[]", OIDACLItem), OIDMacaddrArray: arrayOf("macaddr[]", OIDMacaddr),
	OIDInetArray: arrayOf("inet[]", OIDInet), OIDTimestampArray: arrayOf("timestamp[]", OIDTimestamp),
	OIDDateArray: arrayOf("date[]", OIDDate), OIDTimeArray: arrayOf("time[]", OIDTime),
	OIDTimestampTZArray: arrayOf("timestamptz[]", OIDTimestampTZ),
	OIDIntervalArray:    arrayOf("interval[]", OIDInterval), OIDNumericArray: arrayOf("numeric[]", OIDNumeric),
	OIDTimeTZArray: arrayOf("timetz[]", OIDTimeTZ), OIDBitArray: arrayOf("bit[]", OIDBit),
	OIDVarbitArray: arrayOf("varbit[]", OIDVarbit), OIDRefcursorArray: arrayOf("refcursor[]", OIDRefcursor),
	OIDRegProcedureArray: arrayOf("regprocedure[]", OIDRegProcedure),
	OIDRegOperArray:      arrayOf("regoper[]", OIDRegOper), OIDRegOperatorArray: arrayOf("regoperator[]", OIDRegOperator),
	OIDRegClassArray: arrayOf("regclass[]", OIDRegClass), OIDRegTypeArray: arrayOf("regtype[]", OIDRegType),
	OIDTxidSnapshotArray: arrayOf("txid_snapshot[]", OIDTxidSnapshot), OIDUUIDArray: arrayOf("uuid[]", OIDUUID),
	OIDPgLSNArray: arrayOf("pg_lsn[]", OIDPgLSN), OIDTSVectorArray: arrayOf("tsvector[]", OIDTSVector),
	OIDTSQueryArray: arrayOf("tsquery[]", OIDTSQuery), OIDRegConfigArray: arrayOf("regconfig[]", OIDRegConfig),
	OIDRegDictionaryArray: arrayOf("regdictionary[]", OIDRegDictionary),
	OIDJSONBArray:         arrayOf("jsonb[]", OIDJSONB), OIDJSONArray: arrayOf("json[]", OIDJSON),
	OIDInt4RangeArray: arrayOf("int4range[]", OIDInt4Range), OIDNumRangeArray: arrayOf("numrange[]", OIDNumRange),
	OIDTSRangeArray: arrayOf("tsrange[]", OIDTSRange), OIDTSTZRangeArray: arrayOf("tstzrange[]", OIDTSTZRange),
	OIDDateRangeArray: arrayOf("daterange[]", OIDDateRange), OIDInt8RangeArray: arrayOf("int8range[]", OIDInt8Range),
	OIDJSONPathArray:       arrayOf("jsonpath[]", OIDJSONPath),
	OIDRegNamespaceArray:   arrayOf("regnamespace[]", OIDRegNamespace),
	OIDRegRoleArray:        arrayOf("regrole[]", OIDRegRole),
	OIDRegCollationArray:   arrayOf("regcollation[]", OIDRegCollation),
	OIDPgSnapshotArray:     arrayOf("pg_snapshot[]", OIDPgSnapshot),
	OIDInt4MultirangeArray: arrayOf("int4multirange[]", OIDInt4Multirange),
	OIDNumMultirangeArray:  arrayOf("nummultirange[]", OIDNumMultirange),
	OIDTSMultirangeArray:   arrayOf("tsmultirange[]", OIDTSMultirange),
	OIDTSTZMultirangeArray: arrayOf("tstzmultirange[]", OIDTSTZMultirange),
	OIDDateMultirangeArray: arrayOf("datemultirange[]", OIDDateMultirange),
	OIDInt8MultirangeArray: arrayOf("int8multirange[]", OIDInt8Multirange),
	OIDXID8Array:           arrayOf("xid8[]", OIDXID8), OIDXMLArray: arrayOf("xml[]", OIDXML),
	OIDLineArray: arrayOf("line[]", OIDLine), OIDCIDRArray: arrayOf("cidr[]", OIDCIDR),
	OIDCircleArray: arrayOf("circle[]", OIDCircle), OIDMacaddr8Array: arrayOf("macaddr8[]", OIDMacaddr8),
	OIDMoneyArray: arrayOf("money[]", OIDMoney),
}

// IsKnownType reports whether typeOID is in the mapping table. Values of
// other types are emitted as JSON strings of their text representation; the
// caller counts a warning once per such type.
func IsKnownType(typeOID uint32) bool {
	_, ok := builtinTypes[typeOID]
	return ok
}

// TypeName returns the name of a built-in type in the mapping table as used
// in schema-change events: pg_type.typname for scalars ("int8", "varchar",
// "timestamptz"), "<element>[]" for arrays ("text[]"). Type modifiers are not
// included.
func TypeName(typeOID uint32) (string, bool) {
	info, ok := builtinTypes[typeOID]
	return info.name, ok
}

// FallbackTypeName is the type name used for a type that is neither built in
// nor described by a pgoutput Type message: "oid:<decimal oid>".
func FallbackTypeName(typeOID uint32) string {
	return "oid:" + strconv.FormatUint(uint64(typeOID), 10)
}

// ElementType returns the element type of a built-in array type, the bound
// type of a built-in range type or the range type of a built-in multirange.
func ElementType(typeOID uint32) (uint32, bool) {
	info, ok := builtinTypes[typeOID]
	if !ok || info.elem == 0 {
		return 0, false
	}
	return info.elem, true
}
