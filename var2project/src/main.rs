fn main() {
    let mut a = String::from("hello!");
    a = say(a);
    println!("Printing it again {}", a); //doesn't work if say funtion is not returning any value

}

fn say(s: String) -> String {
    println!("The String is {}", s);
    return "new string".to_string();
}