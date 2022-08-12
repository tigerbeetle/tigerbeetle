pub const Direction = enum {
    ascending,
    descending,

    pub fn reverse(d: Direction) Direction {
        return switch (d) {
            .ascending => .descending,
            .descending => .ascending,
        };
    }
};
