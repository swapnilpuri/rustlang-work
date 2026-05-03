struct Person {
    name: String,
}

fn congratulate (person: &Person) {
    println!("Congratulations, {}!!", person.name);
}


fn main() {
    let person = Person{
        name: String::from("Jack"),
    };
    congratulate(&person);
    println!("Can still use person here: {}", person.name);
}
