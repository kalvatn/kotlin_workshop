package sportradar.kotlin.workshop.examples._11Properties

class Person {
    var name:String ="defaultValue"
    get() {
        println("getting $field")
        return field
    }
    set(value) {
        println(value)
        field = value
    }
}

class Girl {
    var age: Int = 0
        get() = field
        set(value) {
            field = if (value < 18)
                18
            else if (value >= 18 && value <= 30)
                value
            else
                value-3
        }

    var actualAge: Int = 0
}

fun main(args: Array<String>) {

    val p = Person()
    p.name = "jack"
    println("${p.name}")

    val maria = Girl()
    maria.actualAge = 15
    maria.age = 15
    println("Maria: actual age = ${maria.actualAge}")
    println("Maria: pretended age = ${maria.age}")

    val angela = Girl()
    angela.actualAge = 35
    angela.age = 35
    println("Angela: actual age = ${angela.actualAge}")
    println("Angela: pretended age = ${angela.age}")


}