use ironbarrel::record::{HEADER_SIZE, Record, RecordType, decode_header};

#[test]
fn standard_record_round_trips_and_validates() {
    let record = Record::new_standard(b"key".to_vec(), b"value".to_vec()).unwrap();
    let encoded = record.encode().unwrap();
    assert_eq!(encoded.len(), HEADER_SIZE + 3 + 5);
    assert!(!record.is_tombstone());
    assert!(record.verify_crc());

    let mut cursor = encoded.as_slice();
    let header = decode_header(&mut cursor).unwrap().unwrap();
    assert_eq!(header.record_type, u8::from(RecordType::Standard));
    assert_eq!(header.key_sz, 3);
    assert_eq!(header.value_sz, 5);
}

#[test]
fn tombstone_and_targeted_tombstone_have_distinct_shapes() {
    let deletion = Record::new_deletion(b"key".to_vec()).unwrap();
    assert!(deletion.is_tombstone());
    assert_eq!(deletion.header.value_sz, 0);
    assert!(deletion.validate().is_ok());

    let targeted = Record::new_targeted_tombstone(b"key".to_vec(), 7, 42).unwrap();
    assert!(targeted.is_tombstone());
    assert_eq!(targeted.header.value_sz, 16);
    assert!(targeted.validate().is_ok());
}
