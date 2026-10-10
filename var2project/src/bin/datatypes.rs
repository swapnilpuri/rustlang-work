fn main() {

    let a = 5;
    let b = a; // Copy variable
    println!("a: {}, b: {}", a, b);

    let mut c = b; 

   println!("a: {}, c: {}", a, c);
    c= 50;
    println!("a: {}, c: {}", a, &c);
    let d = &c; // Reference variable
    println!("a: {}, c: {}, d: {}", a, c, d);
    //drop(d);
    let _ = d;
    c = 60;
    println!("a: {}, c: {}", a, c);


    

}