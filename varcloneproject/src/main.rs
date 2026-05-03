fn say(s: String) {
    println!("I say {}",s);
}

fn main() {
    let a = String::from("Hello");
    say(a.clone());
    println!("Value of a is {}", a);
}
