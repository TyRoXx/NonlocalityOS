() => {
    # We don't have a keyword for the "Any" type yet. This is a way to name the type:
    let any = type_of({(any_parameter) => any_parameter}(""))
    let a = "bda"
    let b = "lam"
    # type_of is a keyword for getting the type of an expression without evaluating it.
    let f = (x: type_of(a), y: String) => {
        let first = y
        let second = x
        [first, second]
    }
    # Variables can hold types.
    let StringAlias = type_of("")
    let call_function = (function: type_of(f), x: StringAlias, y: String) => function(x, y)
    call_function(f, a, b)
}
