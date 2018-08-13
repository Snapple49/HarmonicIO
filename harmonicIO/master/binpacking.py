from harmonicIO.general.definition import Definition


class BinPacking():

    @staticmethod
    def first_fit(input_list, bin_layout, size_descriptor):
        """
        perform a first fit bin packing on the input list, using the alredy existing list of available bins if provided
        """
        bins = []
        if bin_layout:
            bins = sorted(bin_layout, key=lambda i: i.index)

        # for each item in the list, go through list from left to right and check if it fits in bin and pack it 
        for item in input_list:
            print("Packing item {}".format(item))
            item_packed = False
            for bin_ in bins:
                if bin_.pack(item, size_descriptor):
                    item_packed = True
                    break
                    
            # otherwise make new bin and pack item there
            if not item_packed:
                bins.append(Bin(len(bins)))
                if bins[len(bins)-1].pack(item, size_descriptor):
                    item_packed = True
                    
            print("Item was packed! {}".format(item))
        # lastly check if any pre-existing bins are now empty, and remove
        indices = []
        for i in range(len(bins)):
            if not bins[i].items:
                indices.insert(0, i)
        for i in indices:
            del bins[i]
        
        return bins


class Bin():
    
    class ContainerBinStatus():
        PACKED = "packed"
        QUEUED = "queued"
        RUNNING = "running"
        REQUEUED = "requeued"
    
    class Item():
        def __init__(self, data, size_descriptor=Definition.get_str_size_desc()):
            self.size_descriptor = size_descriptor
            self.size = data[self.size_descriptor]
            self.data = data

        def __str__(self):
            return """Item size: {}
    data: {}""".format(self.size, self.data)

    def __init__(self, bin_index):
        self.items = []
        self.free_space = 1.0
        self.index = bin_index
        self.space_margin = 0.0

    def pack(self, item_data, size_descriptor):
        item = self.Item(item_data, size_descriptor)
        if item.size < self.free_space - self.space_margin:
            item.data["bin_index"] = self.index
            item.data["bin_status"] = self.ContainerBinStatus.PACKED
            self.items.append(item)
            self.free_space -= item.size
            return True
        else:
            del item  
            return False

    def remove_item_in_bin(self, identifier, target):
        for i in range(len(self.items)):
            if self.items[i].data[identifier] == target:
                self.free_space += self.items[i].size
                if self.free_space > 1.0:
                    self.free_space = 1.0
                del self.items[i].data["bin_index"]
                del self.items[i].data["bin_status"]
                del self.items[i]
                return True

        return False

    def update_items_in_bin(self, identifier, update_data):
        for item in self.items:
            if item.data[identifier] == update_data[identifier]:
                self.free_space += item.size
                for field in set(update_data).intersection(item.size_descriptor):
                    item.data[field] = update_data[field]
                    print("______ updated bin: field {} <- data {}".format(field, update_data[field]))
                item.size = item.data[item.size_descriptor]
                self.free_space -= item.size

    def __str__(self):
        bin_items = []
        for item in self.items:
            bin_items.append(str(item))
        return ("""
Bin index: {}, space: {} 
Items:
    {}
""".format(self.index, self.free_space, bin_items))

    