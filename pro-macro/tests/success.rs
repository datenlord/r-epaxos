use pro_macro::FromInner;

#[test]
fn named_struct() {
    #[derive(FromInner)]
    struct NamedStruct {
        inner: usize,
    }

    // unref
    let mut a = NamedStruct { inner: 0 };
    assert_eq!(*a, 0);

    // into
    let b = Into::<NamedStruct>::into(0);
    assert_eq!(*a, *b);

    // value add assign
    a += 1;
    assert_eq!(*a, 1);

    // value add
    let mut a = a + 1;
    assert_eq!(*a, 2);

    // ref add
    let a_ref = &a + 1;
    assert_eq!(*a_ref, 3);

    // mut ref add
    {
        let a_mut_ref = &mut a;
        assert_eq!(*(a_mut_ref + 1), 3);
        assert_eq!(*a, 2);
    }

    // mut ref add assign
    {
        let mut va_mut_ref = &mut a;
        va_mut_ref += 1;
        assert_eq!(**va_mut_ref, 3);
        assert_eq!(*a, 3);
    }
}

#[test]
fn unnamed_struct() {
    #[derive(FromInner)]
    struct UnnamedStruct(usize);

    // unref
    let mut a = UnnamedStruct(0);
    assert_eq!(*a, 0);

    // into
    let b = Into::<UnnamedStruct>::into(0);
    assert_eq!(*a, *b);

    // value add assign
    a += 1;
    assert_eq!(*a, 1);

    // value add
    let mut a = a + 1;
    assert_eq!(*a, 2);

    // ref add
    let a_ref = &a + 1;
    assert_eq!(*a_ref, 3);

    // mut ref add
    {
        let a_mut_ref = &mut a;
        assert_eq!(*(a_mut_ref + 1), 3);
        assert_eq!(*a, 2);
    }

    // mut ref add assign
    {
        let mut va_mut_ref = &mut a;
        va_mut_ref += 1;
        assert_eq!(**va_mut_ref, 3);
        assert_eq!(*a, 3);
    }
}
