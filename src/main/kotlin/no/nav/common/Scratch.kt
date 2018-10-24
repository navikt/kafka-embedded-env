package no.nav.common

fun main(args: Array<String>) {

    val x = mutableListOf<Int>()

    (1..10).asSequence()
            .flatMap { i ->
                println(i)
                listOf(i.also { x.add(i) }).asSequence() }
            .any { it == 4}

    //println((1..5).fold(listOf<Int>()) {r, i -> r + i })
}