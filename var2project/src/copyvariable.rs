fn main() {
    let x: i8 = 5;
    let y: i8 = x + 2;
    println!("x: {}, y: {}", x, y);

    let one = "one".to_string();
    let two = one;
    /** Move String variable*/
    // This will cause a compile error because one is moved to two.
    // Because strings do not implement the Copy trait, the code does not work, 
    //as one was moved to two.
    // println!("{}", one);

    // or use clone() to create a new copy
    //let two: String;
    //two = one.clone();
    //println!("{}", one);
    
    println!("{}", two);

}
    