using System;

namespace TestApp.Models
{
    public class CowModel
    {
        public Guid ID { get; set; }
        public string Breed { get; set; }
        public int Age { get; set; }
        public string Name { get; set; }
        public byte[] DnaData { get; set; }
    }
}
