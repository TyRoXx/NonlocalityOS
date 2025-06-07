() => {
    let a = "bda"
    let b = "lam"
    let f = (x: String, y: String) => {
        let first = y
        let second = x
        [first, second]
    }
    f(a, b)
}