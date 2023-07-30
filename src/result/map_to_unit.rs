pub trait ResultMapExtension<E> {
    fn map_to_unit(self) -> Result<(), E>;
}

impl<T, E> ResultMapExtension<E> for Result<T, E> {
    fn map_to_unit(self) -> Result<(), E> {
        self.map(|_| ())
    }
}
