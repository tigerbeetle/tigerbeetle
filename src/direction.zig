pub const Direction = enum(u1) {
    ascending = 0,
    descending = 1,

    pub fn reverse(d: Direction) Direction {
        return switch (d) {
            .ascending => .descending,
            .descending => .ascending,
        };
    }
};
